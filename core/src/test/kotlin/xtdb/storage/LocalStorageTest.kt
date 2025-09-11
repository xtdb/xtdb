package xtdb.storage

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import xtdb.api.storage.Storage
import xtdb.cache.MemoryCache
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Files.createTempDirectory
import kotlin.io.path.exists
import kotlin.io.path.readBytes

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
            Storage.local(createTempDirectory("local-buffer-pool-test"))
              .open(allocator, memoryCache, null, "xtdb")
    }

    @AfterEach
    fun tearDown() {
        localBufferPool.close()
        memoryCache.close()
        allocator.close()
    }

    @Test
    fun testCopyObject() {
        val localStorage = localBufferPool as LocalStorage
        val testData = "Hello, LocalStorage copyObject test!".toByteArray()
        val srcPath = "test/original.txt".asPath
        val destPath = "test/copy.txt".asPath
        val outsidePath = "../outside/external-copy.txt".asPath
        
        // Put original object
        localStorage.putObject(srcPath, ByteBuffer.wrap(testData))
        
        // Test copying within root directory
        localStorage.copyObject(srcPath, destPath)
        
        // Verify both files exist on disk
        val rootPath = localStorage.rootPath
        val srcFile = rootPath.resolve(srcPath)
        val destFile = rootPath.resolve(destPath)
        
        assertTrue(srcFile.exists(), "Source file should exist")
        assertTrue(destFile.exists(), "Destination file should exist") 
        
        // Verify content is the same
        val srcContent = srcFile.readBytes()
        val destContent = destFile.readBytes()
        assertEquals(testData.contentToString(), srcContent.contentToString())
        assertEquals(testData.contentToString(), destContent.contentToString())
        
        // Test copying outside root directory (using .. to go up)
        localStorage.copyObject(srcPath, outsidePath)
        
        // Verify the file exists outside root
        val outsideFile = rootPath.resolve(outsidePath).normalize()
        assertTrue(outsideFile.exists(), "Outside file should exist")
        
        val outsideContent = outsideFile.readBytes()
        assertEquals(testData.contentToString(), outsideContent.contentToString())
        
        // Verify the outside file is actually outside the root directory
        assertTrue(!outsideFile.startsWith(rootPath), "Outside file should be outside root directory")
    }
}
