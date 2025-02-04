package xtdb.buffer_pool

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.BufferPool
import xtdb.api.storage.SimulatedObjectStore
import xtdb.api.storage.Storage.remoteStorage
import xtdb.api.storage.StoreOperation.COMPLETE
import xtdb.api.storage.StoreOperation.UPLOAD
import xtdb.arrow.I32_TYPE
import xtdb.arrow.Relation
import java.nio.file.Files.createTempDirectory
import java.nio.file.Path
import kotlin.io.path.listDirectoryEntries

class RemoteBufferPoolTest : BufferPoolTest() {
    override fun bufferPool(): BufferPool = remoteBufferPool

    private lateinit var allocator: BufferAllocator
    private lateinit var remoteBufferPool: RemoteBufferPool

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()

        remoteBufferPool =
            remoteStorage(objectStore = { SimulatedObjectStore() }, createTempDirectory("remote-buffer-pool-test"))
                .open(allocator)

        // Mocking small value for MIN_MULTIPART_PART_SIZE
        RemoteBufferPool.minMultipartPartSize = 320
    }

    @AfterEach
    fun tearDown() {
        remoteBufferPool.close()
        allocator.close()
    }


    @Test
    fun arrowIpcTest() {
        val path = Path.of("aw")
        val schema = Schema(listOf(Field("a", FieldType(false, I32_TYPE, null), null)))
        Relation(allocator, schema).use { relation ->
            remoteBufferPool.openArrowWriter(path, relation).use { writer ->
                val v = relation["a"]!!
                for (i in 0 until 10) v.writeInt(i)
                writer.writePage()
                writer.end()
            }
        }
        assertEquals(listOf(UPLOAD, UPLOAD, COMPLETE), (remoteBufferPool.objectStore as SimulatedObjectStore).calls)

        remoteBufferPool.getRecordBatch(path, 0).use { rb ->
            val footer = remoteBufferPool.getFooter(path)
            val rel = Relation.fromRecordBatch(allocator, footer.schema, rb)
            rel.close()
        }
    }

    @Test
    fun bufferPoolClearsUpArrowWriterTempFiles() {
        val rootPath = remoteBufferPool.diskCache.rootPath
        val tmpDir = rootPath.resolve(".tmp")
        val schema = Schema(listOf(Field("a", FieldType(false, I32_TYPE, null), null)))
        Relation(allocator, schema).use { relation ->
            remoteBufferPool.openArrowWriter(Path.of("aw"), relation).use { writer ->
                val v = relation["a"]!!
                for (i in 0 until 10) v.writeInt(i)
                writer.writePage()
                writer.end()
            }
        }

        assertEquals(0, tmpDir.listDirectoryEntries().size)

        val exception = assertThrows(Exception::class.java) {
            Relation(allocator, schema).use { relation ->
                remoteBufferPool.openArrowWriter(Path.of("aw2"), relation).use { writer ->
                    // tmp file present
                    assertEquals(1, tmpDir.listDirectoryEntries().size)

                    val v = relation["a"]!!
                    for (i in 0 until 10) v.writeInt(i)
                    writer.writePage()
                    writer.end()
                    throw Exception("Test exception")
                }
            }
        }
        assertEquals("Test exception", exception.message)

        tmpDir.toFile().listFiles()?.let { assertEquals(0, it.size) }
    }
}
