package xtdb.buffer_pool

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import xtdb.BufferPool

class MemoryBufferPoolTest : BufferPoolTest() {
    override fun bufferPool(): BufferPool  = memoryBufferPool

    private lateinit var allocator: BufferAllocator
    private lateinit var memoryBufferPool: MemoryBufferPool

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        memoryBufferPool = MemoryBufferPool(allocator)
    }

    @AfterEach
    fun tearDown() {
        memoryBufferPool.close()
        allocator.close()
    }
}
