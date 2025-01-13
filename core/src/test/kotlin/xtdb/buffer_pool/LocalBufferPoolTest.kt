package xtdb.buffer_pool

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.IBufferPool
import xtdb.api.storage.Storage
import xtdb.util.requiringResolve
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

class LocalBufferPoolTest : BufferPoolTest() {
    override fun bufferPool(): IBufferPool = localBufferPool

    private lateinit var allocator: BufferAllocator
    private lateinit var localBufferPool: LocalBufferPool


    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        localBufferPool = requiringResolve("xtdb.buffer-pool/open-local-storage").invoke(
            allocator,
            Storage.localStorage(
                Files.createTempDirectory("local-buffer-pool-test")
            ),
            SimpleMeterRegistry()
        ) as LocalBufferPool
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }
}