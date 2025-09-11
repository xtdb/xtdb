package xtdb.storage

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

class MemoryStorageTest : StorageTest() {
    override fun storage(): BufferPool = memoryStorage

    private lateinit var allocator: BufferAllocator
    private lateinit var memoryStorage: MemoryStorage

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        memoryStorage = MemoryStorage(allocator, epoch = 0)
    }

    @AfterEach
    fun tearDown() {
        memoryStorage.close()
        allocator.close()
    }
}
