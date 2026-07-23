package xtdb.storage

import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.PrefixedObjectStore
import xtdb.api.storage.SimulatedObjectStore
import xtdb.api.storage.Storage.remote
import xtdb.api.storage.StoreOperation.COMPLETE
import xtdb.api.storage.StoreOperation.UPLOAD
import xtdb.arrow.Relation
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.schema
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.test.AllocatorResolver
import java.nio.file.Path
import kotlin.io.path.listDirectoryEntries
import com.google.protobuf.Any as ProtoAny

@ExtendWith(AllocatorResolver::class)
class RemoteStorageTest : StorageTest() {
    override fun storage(): BufferPool = remoteBufferPool

    private lateinit var memoryCache: MemoryCache
    private lateinit var sharedBucket: SimulatedObjectStore
    private lateinit var remoteBufferPool: RemoteBufferPool

    // hands every open a prefixing view onto the one shared bucket, the way the cloud stores resolve
    // their prefix over a single durable bucket — so assertions read the pool's real key-space off
    // `sharedBucket` rather than downcasting the pool's client
    class PrefixingObjectStoreFactory(val bucket: SimulatedObjectStore) : ObjectStore.Factory {
        override fun openObjectStore(storageRoot: Path, remotes: Map<RemoteAlias, Remote>): ObjectStore =
            PrefixedObjectStore(storageRoot, bucket)

        override val configProto: ProtoAny
            get() = ProtoAny.newBuilder().build()
    }

    @BeforeEach
    fun setUp(@TempDir localDiskCachePath: Path, al: BufferAllocator) {
        memoryCache = MemoryCache.Factory().open(al)
        sharedBucket = SimulatedObjectStore()
        remoteBufferPool =
            remote(PrefixingObjectStoreFactory(sharedBucket))
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
    fun `openArrowWriter seeds the disk cache under the pool-scoped key`(al: BufferAllocator) {
        val key = Path.of("aw")
        Relation(al, "a" ofType I32).use { relation ->
            remoteBufferPool.openArrowWriter(key, relation).use { writer ->
                val v = relation["a"]
                for (i in 0 until 10) v.writeInt(i)
                writer.writePage()
                writer.end()
            }
        }

        // only the disk cache can serve the read once the store forgets the object
        sharedBucket.buffers.clear()
        assertNotNull(remoteBufferPool.getFooter(key))
    }

    @Test
    fun arrowIpcTest(al: BufferAllocator) = runTest {
        val path = Path.of("aw")
        Relation(al, "a" ofType I32).use { relation ->
            remoteBufferPool.openArrowWriter(path, relation).use { writer ->
                val v = relation["a"]
                for (i in 0 until 10) v.writeInt(i)
                writer.writePage()
                writer.end()
            }
        }
        assertEquals(listOf(UPLOAD, UPLOAD, COMPLETE), sharedBucket.calls)

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
        val schema = schema("a" ofType I32)

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
