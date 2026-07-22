package xtdb.storage

import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.arrow.IntVector
import xtdb.arrow.Relation
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import java.nio.ByteBuffer
import kotlin.concurrent.thread

abstract class StorageTest {
    abstract fun storage(): BufferPool

    /**
     * A pool for one partition of a multi-partition database. Backends with a shared underlying
     * store (local dir, remote object store) return pools over the same root across calls, so the
     * isolation asserted below is real path-scoping, not separate stores.
     */
    abstract fun openPartitionedStorage(partition: Int, totalPartitions: Int): BufferPool

    @Test
    fun listObjectTests_3545() {
        val storage = storage().apply {
            putObject("a/b/c".asPath, ByteBuffer.wrap(ByteArray(10)))
            putObject("a/b/d".asPath, ByteBuffer.wrap(ByteArray(10)))
            putObject("a/e".asPath, ByteBuffer.wrap(ByteArray(10)))
        }

        Thread.sleep(100)

        fun storedObj(path: String) = StoredObject(path.asPath, 10)

        assertEquals(
            listOf(storedObj("a/b/c"), storedObj("a/b/d"), storedObj("a/e")),
            storage.listAllObjects("a".asPath)
        )
        assertEquals(
            listOf(storedObj("a/b/c"), storedObj("a/b/d")),
            storage.listAllObjects("a/b".asPath)
        )
    }

    private fun BufferPool.writeBlock(blockIndex: Long, size: Int = 10) {
        putObject("blocks/b${blockIndex.asLexHex}.binpb".asPath, ByteBuffer.wrap(ByteArray(size)))
    }

    @Test
    fun latestAvailableBlockIndexTest() {
        val storage = storage()
        val blockCount = 50L

        assertNull(storage.latestAvailableBlockIndex(), "no blocks yet")
        assertNull(storage.latestAvailableBlockIndex(null), "no blocks yet, explicit null")

        // pre-populate many blocks, simulating GC-off accumulation
        for (i in 0 until blockCount) {
            storage.writeBlock(i)
        }
        Thread.sleep(100)

        assertEquals(blockCount - 1, storage.latestAvailableBlockIndex(),
            "full listing finds latest block")
        assertEquals(blockCount - 1, storage.latestAvailableBlockIndex(0),
            "afterBlock=0 finds latest block")
        assertEquals(blockCount - 1, storage.latestAvailableBlockIndex(blockCount - 2),
            "afterBlock=penultimate finds latest")
        assertEquals(blockCount - 1, storage.latestAvailableBlockIndex(blockCount - 1),
            "afterBlock=latest, no new blocks, returns latest")

        // add a new block
        storage.writeBlock(blockCount)
        Thread.sleep(100)

        // invalidate cache to simulate TTL expiry — otherwise we'd get stale cached values
        storage.invalidateLatestAvailableBlockCache()

        assertEquals(blockCount, storage.latestAvailableBlockIndex(blockCount - 1),
            "afterBlock=old-latest finds new block")
        assertEquals(blockCount, storage.latestAvailableBlockIndex(blockCount),
            "afterBlock=new-latest, no newer blocks, returns new-latest")
        assertEquals(blockCount, storage.latestAvailableBlockIndex(0),
            "afterBlock=0 still finds latest across all blocks")
    }

    @Test
    fun testArrowFileSize() {
        val bp = storage()
        RootAllocator().use { al ->
            IntVector.open(al, "foo", false).use { fooVec ->
                fooVec.apply { writeInt(10); writeInt(42); writeInt(15) }
                val relation = Relation(al, listOf(fooVec), fooVec.valueCount)
                bp.openArrowWriter("foo".asPath, relation).use { writer ->
                    writer.writePage()
                    assertEquals(526, writer.end())
                }
            }
        }
    }

    @Test
    fun rejectsInvalidPartitionIndex() {
        assertThrows(IllegalArgumentException::class.java) { openPartitionedStorage(2, 2) }
        assertThrows(IllegalArgumentException::class.java) { openPartitionedStorage(-1, 1) }
        assertThrows(IllegalArgumentException::class.java) { openPartitionedStorage(0, 0) }
    }

    @Test
    fun partitionedPoolsAreIsolated() {
        openPartitionedStorage(0, 2).use { p0 ->
            openPartitionedStorage(1, 2).use { p1 ->
                // the same key in both partitions — sizes distinguish who wrote what
                p0.writeBlock(0, size = 3)
                p1.writeBlock(0, size = 7)
                p1.writeBlock(1, size = 7)

                Thread.sleep(100)

                assertEquals(
                    listOf(StoredObject("blocks/b${0L.asLexHex}.binpb".asPath, 3)),
                    p0.listAllObjects().toList(),
                    "partition 0 lists only its own objects"
                )
                assertEquals(
                    listOf(
                        StoredObject("blocks/b${0L.asLexHex}.binpb".asPath, 7),
                        StoredObject("blocks/b${1L.asLexHex}.binpb".asPath, 7),
                    ),
                    p1.listAllObjects().toList(),
                    "partition 1 lists only its own objects"
                )

                assertEquals(0L, p0.latestAvailableBlockIndex(), "block discovery is per-partition")
                assertEquals(1L, p1.latestAvailableBlockIndex(), "block discovery is per-partition")

                // delete is GC's primitive — a partition's GC must never touch a sibling's files
                p0.deleteIfExists("blocks/b${0L.asLexHex}.binpb".asPath)
                Thread.sleep(100)

                assertEquals(emptyList<StoredObject>(), p0.listAllObjects().toList())
                assertEquals(2, p1.listAllObjects().count(), "partition 1 unaffected by partition 0's delete")
            }
        }
    }

    @Test
    fun concurrentPartitionWritesDoNotInterfere() {
        val blockCount = 50L

        openPartitionedStorage(0, 2).use { p0 ->
            openPartitionedStorage(1, 2).use { p1 ->
                listOf(thread { for (i in 0 until blockCount) p0.writeBlock(i, size = 3) },
                       thread { for (i in 0 until blockCount) p1.writeBlock(i, size = 7) })
                    .forEach { it.join() }

                Thread.sleep(100)

                for ((pool, size) in listOf(p0 to 3L, p1 to 7L)) {
                    val objects = pool.listAllObjects().toList()
                    assertEquals(blockCount, objects.size.toLong())
                    assertEquals(setOf(size), objects.map { it.size }.toSet(), "no cross-partition writes")
                    assertEquals(blockCount - 1, pool.latestAvailableBlockIndex())
                }
            }
        }
    }
}
