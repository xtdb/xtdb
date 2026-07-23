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
import xtdb.api.storage.Storage
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
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.io.path.listDirectoryEntries
import com.google.protobuf.Any as ProtoAny

@ExtendWith(AllocatorResolver::class)
class RemoteStorageTest : StorageTest() {
    override fun storage(): BufferPool = remoteBufferPool

    private lateinit var allocator: BufferAllocator
    private lateinit var memoryCache: MemoryCache
    private lateinit var diskCache: DiskCache
    private lateinit var remoteBufferPool: RemoteBufferPool
    private lateinit var sharedObjectStore: SimulatedObjectStore

    object SimulatedObjectStoreFactory : ObjectStore.Factory {
        override fun openObjectStore(storageRoot: Path, remotes: Map<RemoteAlias, Remote>): ObjectStore = SimulatedObjectStore()

        override val configProto: ProtoAny
            get() = ProtoAny.newBuilder().build()
    }

    // one backing store for all opens, each scoped by its storageRoot — the shape the partitioned
    // tests need (the default factory above would give every partition its own store, making
    // cross-partition isolation vacuous)
    class SharedObjectStoreFactory(val store: SimulatedObjectStore) : ObjectStore.Factory {
        override fun openObjectStore(storageRoot: Path, remotes: Map<RemoteAlias, Remote>): ObjectStore =
            PrefixedObjectStore(storageRoot, store)

        override val configProto: ProtoAny
            get() = ProtoAny.newBuilder().build()
    }

    // distinct dbName so the partitioned pools don't alias remoteBufferPool's namespace in the
    // shared memory/disk caches — they're different backing stores that would otherwise share
    // cache keys
    override fun openPartitionedStorage(partition: Int, totalPartitions: Int): BufferPool =
        remote(SharedObjectStoreFactory(sharedObjectStore))
            .open(allocator, memoryCache, diskCache, "parted-db", partition, totalPartitions)

    @BeforeEach
    fun setUp(@TempDir localDiskCachePath: Path, al: BufferAllocator) {
        allocator = al
        memoryCache = MemoryCache.Factory().open(al)
        diskCache = DiskCache.Factory(localDiskCachePath).build()
        sharedObjectStore = SimulatedObjectStore()
        remoteBufferPool =
            remote(SimulatedObjectStoreFactory)
                .open(al, memoryCache, diskCache, "xtdb") as RemoteBufferPool

        // Mocking small value for MIN_MULTIPART_PART_SIZE
        RemoteBufferPool.minMultipartPartSize = 320
    }

    @AfterEach
    fun tearDown() {
        remoteBufferPool.close()
        memoryCache.close()
    }

    @Test
    fun `partitioned pools scope object keys under parts-N`() {
        openPartitionedStorage(0, 2).use { p0 ->
            openPartitionedStorage(1, 2).use { p1 ->
                p0.putObject("blocks/b00.binpb".asPath, ByteBuffer.wrap(ByteArray(3)))
                p1.putObject("blocks/b00.binpb".asPath, ByteBuffer.wrap(ByteArray(7)))
            }
        }

        val versionRoot = Storage.storageRoot(Storage.VERSION, 0)
        assertEquals(
            listOf(
                "parts/0".asPath.resolve(versionRoot).resolve("blocks/b00.binpb"),
                "parts/1".asPath.resolve(versionRoot).resolve("blocks/b00.binpb"),
            ),
            sharedObjectStore.buffers.keys.toList(),
            "raw object-store keys carry the partition marker"
        )
    }

    @Test
    fun `single-partition pool keeps the unmarked key-space`() {
        remote(SharedObjectStoreFactory(sharedObjectStore))
            .open(allocator, memoryCache, diskCache, "xtdb")
            .use { bp -> bp.putObject("blocks/b00.binpb".asPath, ByteBuffer.wrap(ByteArray(10))) }

        assertEquals(
            listOf(Storage.storageRoot(Storage.VERSION, 0).resolve("blocks/b00.binpb")),
            sharedObjectStore.buffers.keys.toList(),
            "no partition marker at partitions = 1"
        )
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
        (remoteBufferPool.objectStore as SimulatedObjectStore).buffers.clear()
        assertNotNull(remoteBufferPool.getFooter(key))
    }

    @Test
    fun `openArrowWriter seeds the disk cache under the pool-scoped key (partitioned pool)`(al: BufferAllocator) {
        openPartitionedStorage(0, 2).use { bp ->
            val key = "aw".asPath
            Relation(al, "a" ofType I32).use { relation ->
                bp.openArrowWriter(key, relation).use { writer ->
                    val v = relation["a"]
                    for (i in 0 until 10) v.writeInt(i)
                    writer.writePage()
                    writer.end()
                }
            }

            // only the disk cache can serve the read once the store forgets the object
            sharedObjectStore.buffers.clear()
            assertNotNull(bp.getFooter(key))
        }
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
