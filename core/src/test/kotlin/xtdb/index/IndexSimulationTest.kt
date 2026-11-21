package xtdb.index

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.SimulationTestUtils.Companion.createJobCalculator
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
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.compactor.CompactorDriverConfig
import xtdb.compactor.CompactorMockDriverFactory
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.garbage_collector.TrieGarbageCollector
import xtdb.log.proto.TrieDetails
import xtdb.metadata.PageMetadata
import xtdb.storage.MemoryStorage
import xtdb.inMemoryLog
import xtdb.symbol
import xtdb.table.IndexFiles
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.BlockIndex
import xtdb.trie.Trie
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

@Tag("property")
class IndexSimulationTest : SimulationTestBase() {
    private val table = TableRef("public", "docs")
    private val garbageLifetime = Duration.ofSeconds(60)
    private val indexPageLimit = 4

    private lateinit var allocator: BufferAllocator
    private lateinit var bp: MemoryStorage
    private lateinit var metadataManager: PageMetadata.Factory
    private lateinit var dataFactory: CompactorMockDriverFactory
    private lateinit var partitionState: PartitionState
    private lateinit var compactor: Compactor
    private lateinit var compactorForDb: Compactor.ForDatabase
    private lateinit var gc: TrieGarbageCollector
    private lateinit var scope: CoroutineScope

    private var clock = Instant.parse("2020-01-01T00:00:00Z")
    private var nextTxId = 0L
    private var latestTxTime: Instant? = null

    private var truth = emptyMap<Long, Set<String>>()

    /** Files a job wrote whose publish the catalog then refused, because the index was dropped meanwhile. */
    private val orphans = mutableMapOf<UUID, MutableSet<Trie.Key>>()

    private inner class IndexingDriver(private val data: Compactor.Driver, private val real: Compactor.Driver) : Compactor.Driver by data {
        override suspend fun executeBackfill(index: IndexDef, block: BlockIndex): IndexBackfilled =
            real.executeBackfill(index, block).also { yield() }

        override suspend fun publishBackfill(backfilled: IndexBackfilled) {
            yield()
            partitionState.indexBackfilled(backfilled.indexId, backfilled.blockIndex, backfilled.l0)
            recordOrphans(backfilled.indexId, listOfNotNull(backfilled.l0))
            announceReady()
        }

        override suspend fun executeIndexCompaction(job: IndexJob): IndexCompacted =
            real.executeIndexCompaction(job).also { yield() }

        override suspend fun publishIndexCompaction(compacted: IndexCompacted) {
            yield()
            partitionState.indexCompacted(compacted.indexId, compacted.outputs, compacted.inputs, clock)
            recordOrphans(compacted.indexId, compacted.outputs)
        }

        override fun close() {
            real.close()
            data.close()
        }
    }

    private fun recordOrphans(id: UUID, written: List<IndexTrie>) {
        val held = partitionState.indexCatalog[id]?.tries?.keys?.map { it.key }.orEmpty()
        written.filter { it.key !in held }.forEach { orphans.getOrPut(id) { mutableSetOf() } += it.key }
    }

    // The leader's announcement, which in a node follows the read-back of a declaration or a backfill.
    private fun announceReady() {
        for (index in partitionState.indexCatalog.indices)
            if ((index.state as? IndexState.Building)?.complete == true) partitionState.indexReady(index.def.id)
    }

    @BeforeEach
    fun setUp() {
        setLogLevel.invoke("xtdb".symbol, "WARN")
        allocator = RootAllocator()
        bp = MemoryStorage(allocator, epoch = 0)
        metadataManager = PageMetadata.Factory(allocator, bp, 16)
        dataFactory = CompactorMockDriverFactory(dispatcher, currentSeed, CompactorDriverConfig())

        // every L1 counts as full, so each side of an index's tree tiers to L2 and beyond
        partitionState = PartitionState.open(allocator, createPartitionValue(fileSizeTarget = 1), ioDispatcher = dispatcher)

        val realFactory = Compactor.Driver.real(indexPageLimit, null)
        val factory = object : Compactor.Driver.Factory {
            override fun create(allocator: BufferAllocator, partitionStorage: PartitionStorage, partitionState: PartitionState, watchers: Watchers) =
                IndexingDriver(
                    dataFactory.create(allocator, partitionStorage, partitionState, watchers),
                    // the real driver won't open without a source log, though its publishes, overridden above, never append
                    realFactory.create(allocator, PartitionStorage(DatabaseLogs(inMemoryLog(), null), bp, metadataManager), partitionState, watchers),
                )
        }

        compactor = Compactor.Impl(factory, null, createJobCalculator(), false, 2, dispatcher = dispatcher, ioDispatcher = dispatcher)
        scope = CoroutineScope(dispatcher)
        compactorForDb = compactor.openForDatabase(
            scope, allocator, PartitionStorage(DatabaseLogs(null, null), bp, null), partitionState,
            Watchers(latestTxId = -1, latestSourceMsgId = -1),
        )

        gc = TrieGarbageCollector(
            bp, partitionState, "xtdb",
            commitTriesDeleted = { t, keys -> partitionState.deleteTries(t, keys) },
            commitIndexTriesDeleted = { id, keys -> partitionState.deleteIndexTries(id, keys) },
            blocksToKeep = 2, garbageLifetime = garbageLifetime, enabled = false, dispatcher = dispatcher,
        )
    }

    @AfterEach
    fun tearDown() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
        compactorForDb.close()
        compactor.close()
        runBlocking { dataFactory.scope.coroutineContext.job.cancelAndJoin() }
        partitionState.close()
        metadataManager.close()
        bp.close()
        allocator.close()
    }

    private fun hashOf(value: Long) = Vector.fromList(allocator, "v", listOf(value)).use { IndexHash.of(it, 0) }

    /** One transaction: a put per row — of `v`, or, for a null, of a document without it — and [indexOp]. */
    private fun commit(rows: List<Pair<ByteArray, Long?>>, indexOp: IndexOp? = null) {
        clock = clock.plusMillis(1)
        val txKey = TransactionKey(nextTxId++, clock)

        if (rows.isEmpty()) {
            partitionState.commitTx(txKey, emptyMap(), indexOp)
        } else {
            Trie.openLogDataWriter(allocator).use { rel ->
                for ((iid, v) in rows) {
                    rel["_iid"].writeBytes(ByteBuffer.wrap(iid))
                    rel["_system_from"].writeLong(clock.asMicros)
                    rel["_valid_from"].writeLong(clock.asMicros)
                    rel["_valid_to"].writeLong(Long.MAX_VALUE)
                    rel["op"].vectorFor("put", STRUCT_TYPE, false).writeObject(if (v == null) mapOf("w" to 0L) else mapOf("v" to v))
                    rel.endRow()
                }
                partitionState.commitTx(txKey, mapOf(table to rel), indexOp)
            }
        }

        // no suspension since the commit, so a snapshot never sees the one without the other
        latestTxTime = clock
        truth = rows.fold(truth) { acc, (iid, v) ->
            if (v == null) acc else hashOf(v).let { h -> acc + (h to (acc[h].orEmpty() + iid.toHex())) }
        }

        if (indexOp is IndexOp.Create) announceReady()
    }

    private suspend fun flushBlock(blockIdx: BlockIndex) {
        val liveIndex = partitionState.liveIndex
        val tableCat = partitionState.tableCatalog

        val finished = liveIndex.finishBlock(bp, blockIdx, tableCat.snap()::slug)
        val tries = finished.mapNotNull { (t, fb) ->
            fb.writtenTrie?.let { written ->
                dataFactory.trieKeyToFileSize[dataFactory.fileSizeKey(t.tableName, written.trieKey)] = written.dataFileSize
                TrieDetails.newBuilder()
                    .setTableName(t.schemaAndTable).setTrieKey(written.trieKey)
                    .setDataFileSize(written.dataFileSize).setTrieMetadata(written.trieMetadata)
                    .build()
            }
        }

        val writer = IndexTrieWriter(allocator, bp, indexPageLimit)
        val indexL0s = partitionState.indexCatalog.indices
            .filter { it.state !is IndexState.Dropped }
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
        bp.putObject(TableCatalog.blockFilePath(blockIdx), ByteBuffer.wrap(block.toByteArray()))

        partitionState.adoptBlock(block, tries, indexL0s, clock)
        compactorForDb.signalBlock()
    }

    private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }

    private fun IndexProbe.iidSet() = iids.mapTo(mutableSetOf()) { it.toHex() }

    private fun probeNow(index: SecondaryIndex, hash: Long) =
        partitionState.openSnapshot(latestTxTime).use { snap -> runBlocking(dispatcher) { IndexProber.probe(allocator, bp, snap, index, hash) } }.iidSet()

    private fun filesOf(id: UUID, keys: Collection<Trie.Key>): Set<String> {
        val files = IndexFiles(partitionState.tableCatalog.snap().slug(table), id)
        return keys.flatMap { listOf(files.dataFilePath(it.toString()), files.metaFilePath(it.toString())) }.mapTo(mutableSetOf()) { it.toString() }
    }

    private fun filesOnDisk(id: UUID): Set<String> {
        val files = IndexFiles(partitionState.tableCatalog.snap().slug(table), id)
        return bp.listAllObjects(files.indexPath).mapTo(mutableSetOf()) { it.key.toString() }
    }

    @RepeatableSimulationTest
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `index probes equal what was put while flushes, backfill, compaction, a drop and gc interleave`() {
        val values = List(24) { rand.nextLong() }
        val hashes = values.map(::hashOf)
        val iids = List(40) { rand.nextBytes(16) }

        val blocks = 32
        val early = IndexDef(UUID(rand.nextLong(), rand.nextLong()), "early", table, "v")
        val late = IndexDef(UUID(rand.nextLong(), rand.nextLong()), "late", table, "v")
        val earlyAt = rand.nextInt(0, blocks / 3)
        val lateAt = rand.nextInt(blocks / 3, 2 * blocks / 3)
        val dropAt = rand.nextInt(earlyAt + 1, blocks + 1)

        var producing = true
        var probes = 0
        var racedProbes = 0

        fun randomRows() = List(rand.nextInt(0, 5)) { iids.random(rand) to values.random(rand).takeIf { rand.nextInt(8) != 0 } }

        runBlocking(dispatcher) {
            val producer = launch {
                for (step in 0..blocks) {
                    val ddl = buildList {
                        if (step == earlyAt) add(IndexOp.Create(early))
                        if (step == lateAt) add(IndexOp.Create(late))
                        if (step == dropAt) add(IndexOp.Drop(early.id, early.name))
                    }
                    val txs = List(rand.nextInt(1, 4)) { randomRows() to null } + ddl.map { emptyList<Pair<ByteArray, Long?>>() to it }

                    for ((rows, op) in txs.shuffled(rand)) {
                        commit(rows, op)
                        repeat(rand.nextInt(0, 3)) { yield() }
                    }

                    // the last step's rows stay in the live tier
                    if (step < blocks) flushBlock(step.toLong())
                }
                producing = false
            }

            val querier = launch {
                while (producing) {
                    val openedAt = clock
                    val held = truth
                    partitionState.openSnapshot(latestTxTime).use { snap ->
                        repeat(rand.nextInt(0, 8)) { yield() }

                        for (index in snap.indexCatSnap.indices.filter { it.state == IndexState.Ready }) {
                            val h = hashes.random(rand)
                            val probed = runCatching { IndexProber.probe(allocator, bp, snap, index, h) }

                            // GC deletes a trie only once it's been Garbage for the grace period, so a probe within it finds every file its snapshot names
                            if (Duration.between(openedAt, clock) < garbageLifetime) {
                                assertEquals(held[h].orEmpty(), probed.getOrThrow().iidSet(), "probe of '${index.def.name}' (seed=$currentSeed)")
                                probes++

                                val liveNow = partitionState.indexCatalog[index.def.id]?.liveTries.orEmpty().toSet()
                                if (!liveNow.containsAll(index.liveTries)) racedProbes++
                            }
                        }
                    }
                    yield()
                }
            }

            val collector = launch {
                while (producing) {
                    clock = clock.plusSeconds(rand.nextLong(1, 30))
                    gc.garbageCollectTries(clock - garbageLifetime)
                    yield()
                }
            }

            producer.join()
            querier.join()
            collector.join()

            compactorForDb.compactAll()
            awaitIdle()
        }

        val lateIndex = partitionState.indexCatalog[late.id]
        assertEquals(IndexState.Ready, lateIndex?.state, "seed=$currentSeed")
        for (h in hashes) assertEquals(truth[h].orEmpty(), probeNow(lateIndex!!, h), "probe at quiesce (seed=$currentSeed)")

        runBlocking(dispatcher) { gc.garbageCollectTries(Instant.parse("3000-01-01T00:00:00Z")) }

        val collectedLate = partitionState.indexCatalog[late.id]!!
        for (h in hashes) assertEquals(truth[h].orEmpty(), probeNow(collectedLate, h), "probe after GC (seed=$currentSeed)")
        assertEquals(collectedLate.tries.keys, collectedLate.liveTries.toSet(), "every Garbage trie is collected (seed=$currentSeed)")
        assertNull(orphans[late.id], "only a drop refuses a job's publish (seed=$currentSeed)")
        assertEquals(filesOf(late.id, collectedLate.tries.keys.map { it.key }), filesOnDisk(late.id), "the late index's files are its catalog's tries (seed=$currentSeed)")

        assertNull(partitionState.indexCatalog[early.id], "the dropped index leaves the catalog (seed=$currentSeed)")
        assertTrue(early.id in partitionState.indexCatalog.collectedIds, "seed=$currentSeed")
        assertEquals(emptySet<String>(), filesOnDisk(early.id), "every file of the dropped index is collected, refused outputs included (seed=$currentSeed)")

        assertTrue(probes > 0, "seed=$currentSeed")
        assertTrue(racedProbes > 0, "no probe held tries a compaction or the drop superseded meanwhile (seed=$currentSeed)")
        assertInterleaved()
    }
}
