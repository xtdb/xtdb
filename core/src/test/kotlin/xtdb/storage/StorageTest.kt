package xtdb.storage

import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.arrow.IntVector
import xtdb.arrow.Relation
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
