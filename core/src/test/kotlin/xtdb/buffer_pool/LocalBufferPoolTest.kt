package xtdb.buffer_pool

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import xtdb.BufferPool
import xtdb.api.storage.Storage.localStorage
import xtdb.cache.MemoryCache
import java.nio.file.Files.createTempDirectory

class LocalBufferPoolTest : BufferPoolTest() {
    private lateinit var allocator: BufferAllocator
    private lateinit var memoryCache: MemoryCache
    private lateinit var localBufferPool: BufferPool

    override fun bufferPool(): BufferPool = localBufferPool

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        memoryCache = MemoryCache.Factory().open(allocator)

        localBufferPool =
            localStorage(createTempDirectory("local-buffer-pool-test"))
                .open(allocator, memoryCache, null)
    }

    @AfterEach
    fun tearDown() {
        localBufferPool.close()
        memoryCache.close()
        allocator.close()
    }
}
