package xtdb.buffer_pool

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import xtdb.IBufferPool

class MemoryBufferPoolTest : BufferPoolTest() {
    override fun bufferPool(): IBufferPool  = memoryBufferPool

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