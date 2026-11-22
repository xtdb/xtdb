package xtdb.index

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.SimulationTestUtils.Companion.createPartitionValue
import xtdb.SimulationTestUtils.Companion.setLogLevel
import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.log.IndexOp
import xtdb.api.log.SourceMessage.IndexBackfilled
import xtdb.api.log.SourceMessage.IndexCompacted
import xtdb.api.log.Watchers
import xtdb.arrow.Relation
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.Vector
import xtdb.block.proto.Block
import xtdb.compactor.Compactor
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.inMemoryLog
import xtdb.log.proto.TrieDetails
import xtdb.metadata.PageMetadata
import xtdb.storage.MemoryStorage
import xtdb.symbol
import xtdb.table.IndexFiles
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.BlockIndex
import xtdb.trie.Trie
import xtdb.trie.TrieCatalog
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

@Tag("property")
class IndexMultiNodeSimulationTest : SimulationTestBase() {
    private val table = TableRef("public", "docs")
    private val indexPageLimit = 4
    private val nodeCount = 3

    private lateinit var allocator: BufferAllocator
    private lateinit var bp: MemoryStorage
    private lateinit var metadataManager: PageMetadata.Factory
    private lateinit var scope: CoroutineScope
    private lateinit var nodes: List<Node>

    private var clock = Instant.parse("2020-01-01T00:00:00Z")
    private var nextTxId = 0L

    private sealed interface Event {
        data class Tx(val key: TransactionKey, val rows: List<Pair<ByteArray, Long?>>, val indexOp: IndexOp?) : Event
        data class BlockAdopted(val block: Block, val tries: List<TrieDetails>, val indexL0s: Map<UUID, IndexTrie>, val at: Instant) : Event
        data class Backfilled(val msg: IndexBackfilled) : Event
        data class Compacted(val msg: IndexCompacted, val at: Instant) : Event
        data class Ready(val id: UUID) : Event
    }

    private val log = MutableStateFlow(emptyList<Event>())

    private data class Entry(val hash: Long, val iid: String)

    private val blockEntries = mutableMapOf<BlockIndex, Set<Entry>>()

    private val covers = mutableMapOf<Pair<UUID, Trie.Key>, Set<BlockIndex>>()

    private val violations = mutableListOf<String>()

    private inner class Node(val idx: Int) : AutoCloseable {
        val state: PartitionState = PartitionState.open(allocator, createPartitionValue(fileSizeTarget = FILE_SIZE_TARGET), ioDispatcher = dispatcher)
        val applied = MutableStateFlow(0)
        private val announced = mutableSetOf<UUID>()

        private val compactor = Compactor.Impl(
            object : Compactor.Driver.Factory {
                override fun create(allocator: BufferAllocator, partitionStorage: PartitionStorage, partitionState: PartitionState, watchers: Watchers) =
                    NodeDriver(this@Node, Compactor.Driver.real(indexPageLimit, null).create(allocator, partitionStorage, partitionState, watchers))
            },
            null, NO_DATA_JOBS, false, 4, dispatcher = dispatcher, ioDispatcher = dispatcher,
        )

        // the real driver won't open without a source log, though its publishes, overridden in NodeDriver, never append
        val forDb: Compactor.ForDatabase = compactor.openForDatabase(
            scope, allocator, PartitionStorage(DatabaseLogs(inMemoryLog(), null), bp, metadataManager), state,
            Watchers(latestTxId = -1, latestSourceMsgId = -1),
        )

        suspend fun append(event: Event) {
            val pos = log.value.size
            log.update { it + event }
            applied.first { it > pos }
        }

        suspend fun runApplier() {
            while (true) {
                val pos = applied.value
                val event = log.first { it.size > pos }[pos]

                repeat(if (rand.nextInt(8) == 0) rand.nextInt(20, 200) else rand.nextInt(0, 4)) { yield() }

                apply(event)
                applied.value = pos + 1
            }
        }

        private suspend fun apply(event: Event) {
            when (event) {
                is Event.Tx -> {
                    openRows(event.key, event.rows).use { rel -> state.commitTx(event.key, if (event.rows.isEmpty()) emptyMap() else mapOf(table to rel), event.indexOp) }
                    if (event.indexOp is IndexOp.Create) announceReady()
                }

                is Event.BlockAdopted -> {
                    state.adoptBlock(event.block, event.tries, event.indexL0s, event.at)
                    forDb.signalBlock()
                }

                is Event.Backfilled -> {
                    state.indexBackfilled(event.msg.indexId, event.msg.blockIndex, event.msg.l0)
                    announceReady()
                }

                is Event.Compacted -> state.indexCompacted(event.msg.indexId, event.msg.outputs, event.msg.inputs, event.at)
                is Event.Ready -> state.indexReady(event.id)
            }
        }

        private fun announceReady() {
            if (idx != 0) return
            for (index in state.indexCatalog.indices)
                if ((index.state as? IndexState.Building)?.complete == true && announced.add(index.def.id))
                    log.update { it + Event.Ready(index.def.id) }
        }

        override fun close() {
            forDb.close()
            compactor.close()
            state.close()
        }
    }

    private inner class NodeDriver(private val node: Node, private val real: Compactor.Driver) : Compactor.Driver by real {
        override suspend fun executeBackfill(index: IndexDef, block: BlockIndex): IndexBackfilled =
            real.executeBackfill(index, block).also { repeat(rand.nextInt(0, 50)) { yield() } }

        override suspend fun publishBackfill(backfilled: IndexBackfilled) {
            yield()
            node.append(Event.Backfilled(backfilled))
        }

        override suspend fun executeIndexCompaction(job: IndexJob): IndexCompacted =
            real.executeIndexCompaction(job).also { checkWritten(job, it) }.also { yield() }

        override suspend fun publishIndexCompaction(compacted: IndexCompacted) {
            yield()
            node.append(Event.Compacted(compacted, clock))
        }
    }

    private fun coverOf(id: UUID, key: Trie.Key): Set<BlockIndex> =
        if (key.level == 0L) setOf(key.blockIndex) else covers.getValue(id to key)

    private fun impliedEntries(key: Trie.Key, cover: Set<BlockIndex>): Set<Entry> =
        cover.flatMapTo(mutableSetOf()) { blockEntries.getValue(it) }.filterTo(mutableSetOf()) { key.partHolds(it.hash) }

    private fun files(id: UUID) = IndexFiles(nodes[0].state.tableCatalog.snap().slug(table), id)

    private suspend fun entriesOnDisk(id: UUID, key: Trie.Key): Set<Entry> {
        val path = files(id).dataFilePath(key.toString())
        val footer = bp.getFooter(path)
        return Relation(allocator, footer.schema).use { rel ->
            footer.recordBatches.indices.flatMapTo(mutableSetOf()) { pageIdx ->
                bp.getRecordBatch(path, pageIdx).use { rel.load(it) }
                List(rel.rowCount) { Entry(rel["_hash"].getLong(it), rel["_iid"].getBytes(it).toHex()) }
            }
        }
    }

    private suspend fun checkWritten(job: IndexJob, compacted: IndexCompacted) {
        val id = job.index.id
        val cover = job.inputs.flatMapTo(sortedSetOf()) { coverOf(id, it) }

        for (output in job.outputs) {
            val prior = covers.putIfAbsent(id to output, cover)
            if (prior != null && prior != cover)
                violations += "$output written from blocks $prior and from blocks $cover (seed=$currentSeed)"
        }

        for (written in compacted.outputs) {
            if (entriesOnDisk(id, written.key) != impliedEntries(written.key, cover))
                violations += "${written.key} doesn't hold the entries of blocks $cover (seed=$currentSeed)"
        }
    }

    @BeforeEach
    fun setUp() {
        setLogLevel.invoke("xtdb".symbol, "WARN")
        allocator = RootAllocator()
        bp = MemoryStorage(allocator, epoch = 0)
        metadataManager = PageMetadata.Factory(allocator, bp, 16)
        scope = CoroutineScope(dispatcher)
        nodes = List(nodeCount) { Node(it) }
    }

    @AfterEach
    fun tearDown() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
        nodes.forEach { it.close() }
        metadataManager.close()
        bp.close()
        allocator.close()
    }

    private fun hashOf(value: Long) = Vector.fromList(allocator, "v", listOf(value)).use { IndexHash.of(it, 0) }

    private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }

    private fun ByteBuffer.toHex() = ByteArray(remaining()).also { duplicate().get(it) }.toHex()

    private fun openRows(key: TransactionKey, rows: List<Pair<ByteArray, Long?>>): Relation =
        Trie.openLogDataWriter(allocator).also { rel ->
            for ((iid, v) in rows) {
                rel["_iid"].writeBytes(ByteBuffer.wrap(iid))
                rel["_system_from"].writeLong(key.systemTime.asMicros)
                rel["_valid_from"].writeLong(key.systemTime.asMicros)
                rel["_valid_to"].writeLong(Long.MAX_VALUE)
                rel["op"].vectorFor("put", STRUCT_TYPE, false).writeObject(if (v == null) mapOf("w" to 0L) else mapOf("v" to v))
                rel.endRow()
            }
        }

    private suspend fun cutBlock(blockIdx: BlockIndex) {
        val leader = nodes[0]
        leader.applied.first { it == log.value.size }

        val liveIndex = leader.state.liveIndex
        val tableCat = leader.state.tableCatalog

        val finished = liveIndex.finishBlock(bp, blockIdx, tableCat.snap()::slug)
        val tries = finished.mapNotNull { (t, fb) ->
            fb.writtenTrie?.let { written ->
                TrieDetails.newBuilder()
                    .setTableName(t.schemaAndTable).setTrieKey(written.trieKey)
                    .setDataFileSize(written.dataFileSize).setTrieMetadata(written.trieMetadata)
                    .build()
            }
        }

        val writer = IndexTrieWriter(allocator, bp, indexPageLimit)
        val indexL0s = leader.state.indexCatalog.indices
            .mapNotNull { index ->
                val liveTable = liveIndex.state.tables[index.def.table] ?: return@mapNotNull null
                writer.writeL0(IndexFiles(tableCat.snap().slug(index.def.table), index.def.id), blockIdx, liveTable.relation, index.def.column)
                    ?.let { index.def.id to it }
            }
            .toMap()

        val block = tableCat.buildBlock(
            blockIdx, liveIndex.state.latestCompletedTx, nextTxId, null,
            tableCat.resolveTables(finished.keys + tableCat.snap().allTables), null,
            termId = 0, boundaryTermSeq = null,
        )

        leader.append(Event.BlockAdopted(block, tries, indexL0s, clock))
    }

    @RepeatableSimulationTest
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `index files written at one key hold the same entries whichever node, at whichever log position, compacts`() {
        val values = List(24) { rand.nextLong() }
        val hashes = values.map(::hashOf)
        val iids = List(40) { rand.nextBytes(16) }

        val blocks = 64
        val index = IndexDef(UUID(rand.nextLong(), rand.nextLong()), "idx", table, "v")
        val createAt = rand.nextInt(blocks / 3, 2 * blocks / 3)

        var truth = emptyMap<Long, Set<String>>()

        fun randomRows() = List(rand.nextInt(0, 6)) { iids.random(rand) to values.random(rand).takeIf { rand.nextInt(8) != 0 } }

        runBlocking(dispatcher) {
            val appliers = nodes.map { node -> scope.launch { node.runApplier() } }

            val producer = launch {
                for (step in 0..blocks) {
                    val txs = List(rand.nextInt(1, 4)) { randomRows() to null } +
                            listOfNotNull((emptyList<Pair<ByteArray, Long?>>() to IndexOp.Create(index)).takeIf { step == createAt })

                    var entries = emptySet<Entry>()
                    for ((rows, op) in txs.shuffled(rand)) {
                        clock = clock.plusMillis(1)
                        log.update { it + Event.Tx(TransactionKey(nextTxId++, clock), rows, op) }

                        for ((iid, v) in rows) if (v != null) {
                            val h = hashOf(v)
                            entries += Entry(h, iid.toHex())
                            truth += h to (truth[h].orEmpty() + iid.toHex())
                        }
                        repeat(rand.nextInt(0, 3)) { yield() }
                    }

                    // the last step's rows stay in the live tier
                    if (step < blocks) {
                        blockEntries[step.toLong()] = entries
                        cutBlock(step.toLong())
                    }
                }
            }

            producer.join()

            do {
                val logged = log.value.size
                nodes.forEach { it.forDb.compactAll() }
                awaitIdle()
            } while (log.value.size != logged || nodes.any { it.applied.value < log.value.size })

            appliers.forEach { it.cancelAndJoin() }
        }

        assertEquals(emptyList<String>(), violations)

        val leaderCat = nodes[0].state.indexCatalog
        for (node in nodes.drop(1))
            assertEquals(leaderCat.indices.toSet(), node.state.indexCatalog.indices.toSet(), "node ${node.idx}'s catalog (seed=$currentSeed)")

        val settled = leaderCat[index.id]!!
        assertEquals(IndexState.Ready, settled.state, "seed=$currentSeed")

        runBlocking(dispatcher) {
            for (trie in settled.liveTries) {
                val cover = coverOf(index.id, trie.key)
                assertEquals(impliedEntries(trie.key, cover), entriesOnDisk(index.id, trie.key), "${trie.key} on disk (seed=$currentSeed)")
            }

            for (node in nodes) {
                val ready = node.state.indexCatalog[index.id]!!
                for (h in hashes) {
                    val probed = node.state.openSnapshot(null).use { snap -> IndexProber.probe(allocator, bp, snap, ready, h) }
                    assertEquals(truth[h].orEmpty(), probed.iids.mapTo(mutableSetOf()) { it.toHex() }, "node ${node.idx}'s probe (seed=$currentSeed)")
                }
            }
        }

        assertTrue(settled.liveTries.any { it.key.level >= 2 }, "the index never tiered (seed=$currentSeed)")
        assertInterleaved()
    }

    private companion object {
        const val FILE_SIZE_TARGET = 3072L

        val NO_DATA_JOBS = object : Compactor.JobCalculator {
            override fun availableJobs(trieCatState: TrieCatalog.State): Collection<Compactor.Job> = emptyList()
        }
    }
}
