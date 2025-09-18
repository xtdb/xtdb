package xtdb.storage

import com.google.protobuf.Any as ProtoAny
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.SimulatedObjectStore
import xtdb.api.storage.Storage.remote
import xtdb.api.storage.StoreOperation.COMPLETE
import xtdb.api.storage.StoreOperation.UPLOAD
import xtdb.arrow.I32
import xtdb.arrow.Relation
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.test.AllocatorResolver
import xtdb.types.Type
import xtdb.types.Type.Companion.ofType
import java.nio.file.Path
import kotlin.io.path.listDirectoryEntries

@ExtendWith(AllocatorResolver::class)
class RemoteStorageTest : StorageTest() {
    override fun storage(): BufferPool = remoteBufferPool

    private lateinit var memoryCache: MemoryCache
    private lateinit var remoteBufferPool: RemoteBufferPool

    object SimulatedObjectStoreFactory : ObjectStore.Factory {
        override fun openObjectStore(storageRoot: Path): ObjectStore = SimulatedObjectStore()

        override val configProto: ProtoAny
            get() = ProtoAny.newBuilder().build()
    }

    @BeforeEach
    fun setUp(@TempDir localDiskCachePath: Path, al: BufferAllocator) {
        memoryCache = MemoryCache.Factory().open(al)
        remoteBufferPool =
            remote(SimulatedObjectStoreFactory)
                .open(al, memoryCache, DiskCache.Factory(localDiskCachePath).build(), "xtdb") as RemoteBufferPool

        // Mocking small value for MIN_MULTIPART_PART_SIZE
        RemoteBufferPool.minMultipartPartSize = 320
    }

    @AfterEach
    fun tearDown() {
        remoteBufferPool.close()
        memoryCache.close()
    }


    @Test
    fun arrowIpcTest(al: BufferAllocator) {
        val path = Path.of("aw")
        Relation(al, "a" ofType Type.I32).use { relation ->
            remoteBufferPool.openArrowWriter(path, relation).use { writer ->
                val v = relation["a"]
                for (i in 0 until 10) v.writeInt(i)
                writer.writePage()
                writer.end()
            }
        }
        assertEquals(listOf(UPLOAD, UPLOAD, COMPLETE), (remoteBufferPool.objectStore as SimulatedObjectStore).calls)

        remoteBufferPool.getRecordBatch(path, 0).use { rb ->
            val footer = remoteBufferPool.getFooter(path)
            val rel = Relation.fromRecordBatch(al, footer.schema, rb)
            rel.close()
        }
    }

    @Test
    fun bufferPoolClearsUpArrowWriterTempFiles(al: BufferAllocator) {
        val rootPath = remoteBufferPool.diskCache.rootPath
        val tmpDir = rootPath.resolve(".tmp")
        val schema = Schema(listOf(Field("a", FieldType(false, I32, null), null)))
        Relation(al, schema).use { relation ->
            remoteBufferPool.openArrowWriter(Path.of("aw"), relation).use { writer ->
                val v = relation["a"]
                for (i in 0 until 10) v.writeInt(i)
                writer.writePage()
                writer.end()
            }
        }

        assertEquals(0, tmpDir.listDirectoryEntries().size)

        val exception = assertThrows(Exception::class.java) {
            Relation(al, schema).use { relation ->
                remoteBufferPool.openArrowWriter(Path.of("aw2"), relation).use { writer ->
                    // tmp file present
                    assertEquals(1, tmpDir.listDirectoryEntries().size)

                    val v = relation["a"]
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
