package xtdb.storage

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import xtdb.api.storage.Storage.localStorage
import xtdb.cache.MemoryCache
import java.nio.file.Files.createTempDirectory

class LocalStorageTest : StorageTest() {
    private lateinit var allocator: BufferAllocator
    private lateinit var memoryCache: MemoryCache
    private lateinit var localBufferPool: BufferPool

    override fun storage(): BufferPool = localBufferPool

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        memoryCache = MemoryCache.Factory().open(allocator)

        localBufferPool =
            localStorage(createTempDirectory("local-buffer-pool-test"))
                .open(allocator, memoryCache, null, "xtdb")
    }

    @AfterEach
    fun tearDown() {
        localBufferPool.close()
        memoryCache.close()
        allocator.close()
    }
}
