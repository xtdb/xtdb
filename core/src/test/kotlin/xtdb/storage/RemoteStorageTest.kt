package xtdb.storage

import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
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
import xtdb.api.storage.InMemoryBucket
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.PrefixedObjectStore
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
class RemoteStorageTest : PartitionedStorageTest() {
    override fun storage(): BufferPool = remoteBufferPool

    private lateinit var allocator: BufferAllocator
    private lateinit var memoryCache: MemoryCache
    private lateinit var diskCache: DiskCache
    private lateinit var remoteBufferPool: RemoteBufferPool
    private lateinit var xtdbBucket: InMemoryBucket
    private lateinit var partedBucket: InMemoryBucket

    // hands every open a prefixing view onto one shared bucket, the way the cloud stores resolve their
    // prefix over a single durable bucket — so assertions read the pool's real key-space off the bucket
    // rather than downcasting the pool's client
    class PrefixingObjectStoreFactory(val bucket: InMemoryBucket) : ObjectStore.Factory {
        override fun openObjectStore(storageRoot: Path, remotes: Map<RemoteAlias, Remote>): ObjectStore =
            PrefixedObjectStore(storageRoot, bucket)

        override val configProto: ProtoAny
            get() = ProtoAny.newBuilder().build()
    }

    // partitioned pools get their own bucket and a distinct dbName, so they neither alias
    // remoteBufferPool's namespace in the node-shared caches nor pollute its key-space observations
    override fun openPartitionedStorage(partition: Int, totalPartitions: Int): BufferPool =
        remote(PrefixingObjectStoreFactory(partedBucket))
            .open(allocator, memoryCache, diskCache, "parted-db", partition, totalPartitions)

    @BeforeEach
    fun setUp(@TempDir localDiskCachePath: Path, al: BufferAllocator) {
        allocator = al
        memoryCache = MemoryCache.Factory().open(al)
        diskCache = DiskCache.Factory(localDiskCachePath).build()
        xtdbBucket = InMemoryBucket()
        partedBucket = InMemoryBucket()
        remoteBufferPool =
            remote(PrefixingObjectStoreFactory(xtdbBucket))
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
            partedBucket.buffers.keys.toList(),
            "raw object-store keys carry the partition marker"
        )
    }

    @Test
    fun `single-partition pool keeps the unmarked key-space`() {
        openPartitionedStorage(0, 1).use { bp ->
            bp.putObject("blocks/b00.binpb".asPath, ByteBuffer.wrap(ByteArray(10)))
        }

        assertEquals(
            listOf(Storage.storageRoot(Storage.VERSION, 0).resolve("blocks/b00.binpb")),
            partedBucket.buffers.keys.toList(),
            "no partition marker at partitions = 1"
        )
    }

    /**
     * Lives here rather than in [PartitionedStorageTest] because only the remote backend can express it:
     * the disk cache is durable, so p0's entry is still there when p1 reads. Local has no disk cache, and
     * MemoryCache releases an entry once the last reference drops, so p1's read is a fresh miss that
     * reloads from its own root — the assertion would pass there even with the partition dropped from the
     * cache key. Verified by reverting `cacheRootPath` to `dbName/0`: this fails, the local sibling didn't.
     */
    @Test
    fun `partitions get their own entries in the node-shared caches`() {
        openPartitionedStorage(0, 2).use { p0 ->
            openPartitionedStorage(1, 2).use { p1 ->
                val key = "blocks/b00.binpb".asPath
                p0.putObject(key, ByteBuffer.wrap(ByteArray(3)))
                p1.putObject(key, ByteBuffer.wrap(ByteArray(7)))

                // p0 reads first, seeding the shared cache under this key
                assertEquals(3, p0.getByteArray(key).size, "partition 0 reads its own bytes")
                assertEquals(7, p1.getByteArray(key).size, "partition 1 isn't served partition 0's cache entry")
            }
        }
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
        xtdbBucket.buffers.clear()
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
        assertEquals(listOf(UPLOAD, UPLOAD, COMPLETE), xtdbBucket.calls)

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
