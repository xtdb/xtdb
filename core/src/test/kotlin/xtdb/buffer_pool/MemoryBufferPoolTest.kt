package xtdb.buffer_pool

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.IBufferPool
import xtdb.util.requiringResolve
import java.nio.ByteBuffer
import java.nio.file.Path

class MemoryBufferPoolTest : BufferPoolTest() {
    override fun bufferPool(): IBufferPool  = memoryBufferPool

    private lateinit var allocator: BufferAllocator
    private lateinit var memoryBufferPool: MemoryBufferPool


    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        memoryBufferPool = requiringResolve("xtdb.buffer-pool/open-in-memory-storage").invoke(
            allocator,
            SimpleMeterRegistry()
        ) as MemoryBufferPool
    }

    @AfterEach
    fun tearDown() {
        memoryBufferPool.close()
        allocator.close()
    }
}