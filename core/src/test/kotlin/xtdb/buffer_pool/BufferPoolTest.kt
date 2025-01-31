package xtdb.buffer_pool

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.BufferPool
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path

abstract class BufferPoolTest {
    abstract fun bufferPool() : BufferPool

    @Test
    fun listObjectTests_3545() {
        val bufferPool = bufferPool().apply {
            putObject(Path.of("a/b/c"), ByteBuffer.wrap(ByteArray(10)))
            putObject(Path.of("a/b/d"), ByteBuffer.wrap(ByteArray(10)))
            putObject(Path.of("a/e"), ByteBuffer.wrap(ByteArray(10)))
        }

        Thread.sleep(100)

        assertEquals(listOf("a/b/c".asPath, "a/b/d".asPath, "a/e".asPath), bufferPool.listObjects("a".asPath))
        assertEquals(listOf("a/b/c".asPath, "a/b/d".asPath), bufferPool.listObjects("a/b".asPath))
    }
}
