package xtdb.buffer_pool

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.IBufferPool
import java.nio.ByteBuffer
import java.nio.file.Path

abstract class BufferPoolTest {
    abstract fun bufferPool() : IBufferPool

    @Test
    fun listObjectTests_3545() {
        val bufferPool = bufferPool()
        bufferPool.putObject(Path.of("a/b/c"), ByteBuffer.wrap(ByteArray(10)))
        bufferPool.putObject(Path.of("a/b/d"), ByteBuffer.wrap(ByteArray(10)))
        bufferPool.putObject(Path.of("a/e"), ByteBuffer.wrap(ByteArray(10)))

        assertEquals(listOf(Path.of("a/b"), Path.of("a/e")), bufferPool.listObjects(Path.of("a")))
    }
}