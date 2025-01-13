package xtdb.buffer_pool

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
import xtdb.IBufferPool
import xtdb.api.log.FileListCache
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.SimulatedObjectStore
import xtdb.api.storage.Storage
import xtdb.api.storage.StoreOperation.COMPLETE
import xtdb.api.storage.StoreOperation.UPLOAD
import xtdb.arrow.I32_TYPE
import xtdb.arrow.Relation
import xtdb.util.requiringResolve
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

class RemoteBufferPoolTest : BufferPoolTest() {
    override fun bufferPool(): IBufferPool  = remoteBufferPool

    private lateinit var allocator: BufferAllocator
    private lateinit var remoteBufferPool: RemoteBufferPool

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        remoteBufferPool = requiringResolve("xtdb.buffer-pool/open-remote-storage").invoke(
            allocator,
            Storage.remoteStorage(
                object : ObjectStore.Factory {
                    override fun openObjectStore(): ObjectStore = SimulatedObjectStore()
                },
                Files.createTempDirectory("remote-buffer-pool-test")
                ),
            FileListCache.SOLO,
            SimpleMeterRegistry()) as RemoteBufferPool
        // Mocking small value for MIN_MULTIPART_PART_SIZE
        RemoteBufferPool.setMinMultipartPartSize(320)
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
                val v = relation.get("a")!!
                for (i in 0 until 10) {
                    v.writeInt(i)
                }
                writer.writeBatch()
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
                val v = relation.get("a")!!
                for (i in 0 until 10) {
                    v.writeInt(i)
                }
                writer.writeBatch()
                writer.end()
            }
        }
        tmpDir.toFile().listFiles()?.let { assertEquals(0, it.size) }

        val exception = assertThrows(Exception::class.java) {
            Relation(allocator, schema).use { relation ->
                remoteBufferPool.openArrowWriter(Path.of("aw2"), relation).use { writer ->
                    // tmp file present
                    tmpDir.toFile().listFiles()?.let { assertEquals(1, it.size) }

                    val v = relation.get("a")!!
                    for (i in 0 until 10) {
                        v.writeInt(i)
                    }
                    writer.writeBatch()
                    writer.end()
                    throw Exception("Test exception")
                }
            }
        }
        assertEquals("Test exception", exception.message)

        tmpDir.toFile().listFiles()?.let { assertEquals(0, it.size) }
    }
}