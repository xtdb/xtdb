package xtdb.buffer_pool

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.BufferPool
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.util.asPath
import java.nio.ByteBuffer

abstract class BufferPoolTest {
    abstract fun bufferPool(): BufferPool

    @Test
    fun listObjectTests_3545() {
        val bufferPool = bufferPool().apply {
            putObject("a/b/c".asPath, ByteBuffer.wrap(ByteArray(10)))
            putObject("a/b/d".asPath, ByteBuffer.wrap(ByteArray(10)))
            putObject("a/e".asPath, ByteBuffer.wrap(ByteArray(10)))
        }

        Thread.sleep(100)

        fun storedObj(path: String) = StoredObject(path.asPath, 10)

        assertEquals(
            listOf(storedObj("a/b/c"), storedObj("a/b/d"), storedObj("a/e")),
            bufferPool.listObjects("a".asPath)
        )
        assertEquals(
            listOf(storedObj("a/b/c"), storedObj("a/b/d")),
            bufferPool.listObjects("a/b".asPath)
        )
    }
}
