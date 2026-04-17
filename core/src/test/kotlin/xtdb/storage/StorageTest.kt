package xtdb.storage

import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.arrow.IntVector
import xtdb.arrow.Relation
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import java.nio.ByteBuffer

abstract class StorageTest {
    abstract fun storage(): BufferPool

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

    private fun BufferPool.writeBlock(blockIndex: Long) {
        putObject("blocks/b${blockIndex.asLexHex}.binpb".asPath, ByteBuffer.wrap(ByteArray(10)))
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
}
