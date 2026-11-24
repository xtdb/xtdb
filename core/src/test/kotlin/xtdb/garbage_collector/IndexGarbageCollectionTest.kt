package xtdb.garbage_collector

import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.SimulationTestUtils.Companion.createPartitionValue
import xtdb.api.TableRef
import xtdb.database.PartitionState
import xtdb.index.IndexCatalog
import xtdb.index.IndexDef
import xtdb.index.IndexTrie
import xtdb.index.SecondaryIndex
import xtdb.storage.MemoryStorage
import xtdb.table.IndexFiles
import xtdb.table.TableSlug
import xtdb.trie.Trie
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.util.UUID

class IndexGarbageCollectionTest {
    private lateinit var al: BufferAllocator
    private lateinit var bp: MemoryStorage

    private val byEmail = IndexDef(UUID.randomUUID(), "by_email", TableRef("public", "users"), "email")
    private val files = IndexFiles(TableSlug.of(byEmail.table), byEmail.id)

    private val l0s = (0L..1L).map { IndexTrie(Trie.l0Key(it), 10) }
    private val l1 = IndexTrie(Trie.Key(1, null, null, 1), 20)
    private val compactedAt = Instant.parse("2026-01-01T00:00:00Z")

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
        bp = MemoryStorage(al, epoch = 0)
    }

    @AfterEach
    fun tearDown() {
        bp.close()
        al.close()
    }

    private fun put(files: IndexFiles, key: Trie.Key) {
        bp.putObjectSync(files.dataFilePath(key.toString()), ByteBuffer.allocate(1))
        bp.putObjectSync(files.metaFilePath(key.toString()), ByteBuffer.allocate(1))
    }

    private fun partitionWith(catalog: IndexCatalog): PartitionState {
        for (index in catalog.indices)
            for (trie in index.tries.keys) put(IndexFiles(files.table, index.def.id), trie.key)

        return PartitionState(createPartitionValue().withIndexCat(catalog))
    }

    private fun gcOf(partition: PartitionState) =
        TrieGarbageCollector(
            bp, partition, "xtdb",
            commitTriesDeleted = { table, keys -> partition.deleteTries(table, keys) },
            commitIndexTriesDeleted = { id, keys -> partition.deleteIndexTries(id, keys) },
            blocksToKeep = 1, garbageLifetime = Duration.ZERO, enabled = false,
        )

    private fun filesOnDisk() = bp.listAllObjects(files.table.tablePath).map { it.key.toString() }.toSet()

    private fun filesOf(tries: Collection<IndexTrie>, files: IndexFiles = this.files) =
        tries.flatMap { listOf(files.dataFilePath(it.key.toString()).toString(), files.metaFilePath(it.key.toString()).toString()) }.toSet()

    private val compacted =
        IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet())
            .withL0s(mapOf(byEmail.id to l0s[0])).withL0s(mapOf(byEmail.id to l0s[1]))
            .compacted(byEmail.id, listOf(l1), l0s.map { it.key }, compactedAt)

    @Test
    fun `an index's superseded files, L0s included, are deleted once past the cutoff, and every node's catalog drops them`() = runBlocking {
        val partition = partitionWith(compacted)
        val gc = gcOf(partition)

        gc.garbageCollectTries(compactedAt.minusNanos(1000))
        assertEquals(filesOf(l0s + l1), filesOnDisk(), "within the grace period, nothing is deleted")
        assertEquals(compacted.indices.toList(), partition.indexCatalog.indices.toList())

        gc.garbageCollectTries(compactedAt)
        assertEquals(filesOf(listOf(l1)), filesOnDisk())
        assertEquals(listOf(l1), partition.indexCatalog[byEmail.id]?.tries?.keys?.toList(), "the files on disk are the catalog's tries")
    }

    @Test
    fun `a dropped index's every file is collected once past the drop's cutoff, and then it leaves the catalog`() = runBlocking {
        val droppedAt = compactedAt.plusSeconds(60)
        val partition = partitionWith(compacted.dropped(byEmail.id, droppedAt))
        val gc = gcOf(partition)

        gc.garbageCollectTries(droppedAt.minusNanos(1000))
        assertEquals(filesOf(listOf(l1)), filesOnDisk(), "the compaction's inputs are past the cutoff, the drop's tries aren't")
        assertEquals(listOf(byEmail.id), partition.indexCatalog.indices.map(SecondaryIndex::def).map(IndexDef::id))

        gc.garbageCollectTries(droppedAt)
        assertEquals(emptySet<String>(), filesOnDisk())
        assertEquals(emptyList<SecondaryIndex>(), partition.indexCatalog.indices.toList())
        assertEquals(setOf(byEmail.id), partition.indexCatalog.collectedIds, "its id stays, so its declaration can't be replayed")
    }

    @Test
    fun `collecting a dropped index's last trie deletes the files no catalog names under its directory, and leaves a live index's alone`() = runBlocking {
        val byName = IndexDef(UUID.randomUUID(), "by_name", byEmail.table, "name")
        val byNameFiles = IndexFiles(files.table, byName.id)
        val droppedAt = compactedAt.plusSeconds(60)

        val catalog = compacted.dropped(byEmail.id, droppedAt)
            .withIndex(byName, 0, emptySet()).withL0s(mapOf(byName.id to l0s[0]))
        val partition = partitionWith(catalog)

        val refusedOutput = Trie.Key(2, null, null, 1)
        val lateBackfillL0 = Trie.l0Key(5)
        put(files, refusedOutput)
        put(files, lateBackfillL0)
        val stray = Trie.Key(1, null, null, 0)
        put(byNameFiles, stray)

        gcOf(partition).garbageCollectTries(droppedAt)

        assertEquals(filesOf(listOf(l0s[0], IndexTrie(stray, 1)), byNameFiles), filesOnDisk())
        assertEquals(setOf(byEmail.id), partition.indexCatalog.collectedIds)
    }

    @Test
    fun `an index dropped holding no tries is swept and collected by the first cycle past the drop's cutoff`() = runBlocking {
        val droppedAt = compactedAt
        val partition = partitionWith(IndexCatalog.EMPTY.withIndex(byEmail, 1, setOf(0L)).dropped(byEmail.id, droppedAt))
        val gc = gcOf(partition)

        val lateBackfillL0 = Trie.l0Key(0)
        put(files, lateBackfillL0)

        gc.garbageCollectTries(droppedAt.minusNanos(1000))
        assertEquals(filesOf(listOf(IndexTrie(lateBackfillL0, 1))), filesOnDisk(), "within the grace period, nothing is swept")
        assertEquals(listOf(byEmail.id), partition.indexCatalog.indices.map(SecondaryIndex::def).map(IndexDef::id))

        gc.garbageCollectTries(droppedAt)
        assertEquals(emptySet<String>(), filesOnDisk())
        assertEquals(emptyList<SecondaryIndex>(), partition.indexCatalog.indices.toList())
        assertEquals(setOf(byEmail.id), partition.indexCatalog.collectedIds)
    }
}
