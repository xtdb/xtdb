package xtdb.storage

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import xtdb.api.storage.Storage
import xtdb.cache.MemoryCache

class MemoryStorageTest : StorageTest() {
    override fun storage(): BufferPool = memoryStorage

    override fun openPartitionedStorage(partition: Int, totalPartitions: Int): BufferPool =
        Storage.inMemory().open(allocator, memoryCache, null, "xtdb", partition, totalPartitions)

    private lateinit var allocator: BufferAllocator
    private lateinit var memoryCache: MemoryCache
    private lateinit var memoryStorage: MemoryStorage

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        memoryCache = MemoryCache.Factory().open(allocator)
        memoryStorage = MemoryStorage(allocator, epoch = 0)
    }

    @AfterEach
    fun tearDown() {
        memoryStorage.close()
        memoryCache.close()
        allocator.close()
    }
}
