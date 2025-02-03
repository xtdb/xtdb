package xtdb.buffer_pool

import com.github.benmanes.caffeine.cache.Cache
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.BufferPool
import xtdb.IEvictBufferTest
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.Storage
import xtdb.api.storage.Storage.LocalStorageFactory
import xtdb.api.storage.Storage.arrowFooterCache
import xtdb.api.storage.Storage.openStorageChildAllocator
import xtdb.api.storage.Storage.registerMetrics
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.ArrowUtil.toByteArray
import xtdb.arrow.Relation
import xtdb.cache.MemoryCache
import xtdb.cache.PathSlice
import xtdb.trie.FileSize
import xtdb.util.closeOnCatch
import xtdb.util.maxDirectMemory
import xtdb.util.newSeekableByteChannel
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.file.Files.newByteChannel
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.CompletableFuture.completedFuture
import kotlin.io.path.*

class LocalBufferPool(
    allocator: BufferAllocator,
    factory: LocalStorageFactory,
    meterRegistry: MeterRegistry = SimpleMeterRegistry()
) : BufferPool, IEvictBufferTest, Closeable {

    private val allocator = allocator.openStorageChildAllocator().also { it.registerMetrics(meterRegistry) }

    private val memoryCache =
        MemoryCache(allocator, factory.maxCacheBytes ?: (maxDirectMemory / 2))
            .also { it.registerMetrics("memory-cache", meterRegistry) }

    private val diskStore = factory.path.resolve(Storage.storageRoot).also { it.createDirectories() }

    private val arrowFooterCache: Cache<Path, ArrowFooter> = arrowFooterCache()
    private val recordBatchRequests: Counter = meterRegistry.counter("record-batch-requests")
    private val memCacheMisses: Counter = meterRegistry.counter("mem-cache-misses")

    companion object {
        private fun Path.createTempUploadFile(): Path {
            val tmpDir = resolve(".tmp").also { it.createDirectories() }
            return createTempFile(tmpDir, "upload", ".arrow")
        }

        private fun objectMissingException(path: Path) = IllegalStateException("Object $path doesn't exist.")

        private fun Path.orThrowIfMissing(key: Path) = takeIf { it.exists() } ?: throw objectMissingException(key)
    }

    override fun getByteArray(key: Path): ByteArray =
        memoryCache.get(PathSlice(key)) { pathSlice ->
            memCacheMisses.increment()
            val bufferCachePath = diskStore.resolve(pathSlice.path).orThrowIfMissing(key)

            completedFuture(Pair(PathSlice(bufferCachePath, pathSlice.offset, pathSlice.length), null))
        }.use { it.toByteArray() }

    override fun getFooter(key: Path): ArrowFooter =
        arrowFooterCache.get(key) {
            val path = diskStore.resolve(key).orThrowIfMissing(key)

            Relation.readFooter(path.newSeekableByteChannel())
        }

    override fun getRecordBatch(key: Path, blockIdx: Int): ArrowRecordBatch {
        recordBatchRequests.increment()
        val path = diskStore.resolve(key).orThrowIfMissing(key)

        val footer = arrowFooterCache.get(key) { Relation.readFooter(path.newSeekableByteChannel()) }

        val block = footer.recordBatches.getOrNull(blockIdx)
            ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

        return memoryCache.get(
            PathSlice(key, block.offset, block.metadataLength + block.bodyLength)
        ) { pathSlice ->
            memCacheMisses.increment()
            val bufferCachePath =
                diskStore.resolve(pathSlice.path)
                    .takeIf { it.exists() } ?: throw objectMissingException(path)

            completedFuture(Pair(PathSlice(bufferCachePath, pathSlice.offset, pathSlice.length), null))
        }.use { arrowBuf ->
            arrowBuf.arrowBufToRecordBatch(
                0, block.metadataLength, block.bodyLength, "Failed opening record batch '$path' at block-idx $blockIdx"
            )
        }
    }

    private fun ByteBuffer.writeToPath(path: Path) {
        newByteChannel(path, WRITE, TRUNCATE_EXISTING, CREATE).use { channel ->
            while (hasRemaining()) channel.write(this)
        }
    }

    override fun putObject(key: Path, buffer: ByteBuffer) {
        try {
            val tmpPath = diskStore.createTempUploadFile()
            buffer.writeToPath(tmpPath)

            val filePath = diskStore.resolve(key).also { it.createParentDirectories() }
            tmpPath.moveTo(filePath, StandardCopyOption.ATOMIC_MOVE)
        } catch (e: ClosedByInterruptException) {
            throw InterruptedException()
        }
    }

    private fun Path.listAll() = walk()
        .map { StoredObject(diskStore.relativize(it), it.fileSize()) }
        .filter { it.key.getName(0).toString() != ".tmp" }
        .sortedBy { it.key }
        .toList()

    override fun listAllObjects(): Iterable<StoredObject> = diskStore.listAll()
    override fun listAllObjects(dir: Path) = diskStore.resolve(dir).listAll()

    override fun openArrowWriter(key: Path, rel: Relation): xtdb.ArrowWriter {
        val tmpPath = diskStore.createTempUploadFile()
        return newByteChannel(tmpPath, WRITE, TRUNCATE_EXISTING, CREATE).closeOnCatch { fileChannel ->
            rel.startUnload(fileChannel).closeOnCatch { unloader ->
                object : xtdb.ArrowWriter {
                    override fun writeBatch() {
                        try {
                            unloader.writeBatch()
                        } catch (e: ClosedByInterruptException) {
                            throw InterruptedException()
                        }
                    }

                    override fun end(): FileSize {
                        unloader.end()
                        fileChannel.close()

                        val filePath = diskStore.resolve(key).also { it.createParentDirectories() }
                        tmpPath.moveTo(filePath, StandardCopyOption.ATOMIC_MOVE)
                        return filePath.fileSize()
                    }

                    override fun close() {
                        unloader.close()
                        if (fileChannel.isOpen) fileChannel.close()
                        tmpPath.deleteIfExists()
                    }
                }
            }
        }
    }

    override fun evictCachedBuffer(key: Path) {
        memoryCache.invalidate(PathSlice(key))
    }

    override fun close() {
        memoryCache.close()
        allocator.close()
    }
}
