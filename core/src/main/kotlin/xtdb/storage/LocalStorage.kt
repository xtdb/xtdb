package xtdb.storage

import com.github.benmanes.caffeine.cache.Cache
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.ArrowWriter
import xtdb.IEvictBufferTest
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.Storage.arrowFooterCache
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.ArrowUtil.readArrowFooter
import xtdb.arrow.ArrowUtil.toByteArray
import xtdb.arrow.Relation
import xtdb.cache.MemoryCache
import xtdb.cache.PathSlice
import xtdb.database.DatabaseName
import xtdb.trie.FileSize
import xtdb.util.*
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.file.Files.newByteChannel
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.CompletableFuture.completedFuture
import kotlin.io.path.*

internal class LocalStorage(
    allocator: BufferAllocator,
    private val memoryCache: MemoryCache,
    meterRegistry: MeterRegistry? = null,
    dbName: DatabaseName,
    private val diskStore: Path,
) : BufferPool, IEvictBufferTest, Closeable {

    private val allocator = allocator.openChildAllocator("buffer-pool").also { meterRegistry?.register(it) }

    private val arrowFooterCache: Cache<Path, ArrowFooter> = arrowFooterCache()
    private val recordBatchRequests: Counter? = meterRegistry?.counter("record-batch-requests")
    private val memCacheMisses: Counter? = meterRegistry?.counter("memory-cache-misses")

    // we partition the cache by dbName as it's shared between multiple databases
    // the cache itself has no knowledge of this
    // '0' for partition 0, in advance of multi-partition support
    private val cacheRootPath = dbName.asPath.resolve("0")

    companion object {
        private fun Path.createTempUploadFile(): Path {
            val tmpDir = resolve(".tmp").also { it.createDirectories() }
            return createTempFile(tmpDir, "upload", ".arrow")
        }

        private fun objectMissingException(path: Path) = IllegalStateException("Object $path doesn't exist.")

        private fun Path.orThrowIfMissing(key: Path) = takeIf { it.exists() } ?: throw objectMissingException(key)
    }

    override fun getByteArray(key: Path): ByteArray =
        memoryCache.get(PathSlice(cacheRootPath.resolve(key))) { pathSlice ->
            memCacheMisses?.increment()
            val bufferCachePath = diskStore
                .resolve(cacheRootPath.relativize(pathSlice.path))
                .orThrowIfMissing(key)

            completedFuture(Pair(PathSlice(bufferCachePath, pathSlice.offset, pathSlice.length), null))
        }.use { it.toByteArray() }

    override fun getFooter(key: Path): ArrowFooter =
        arrowFooterCache.get(key) {
            val path = diskStore.resolve(key).orThrowIfMissing(key)

            path.openReadableChannel().readArrowFooter()
        }

    override fun getRecordBatch(key: Path, idx: Int): ArrowRecordBatch {
        recordBatchRequests?.increment()
        val path = diskStore.resolve(key).orThrowIfMissing(key)

        val footer = arrowFooterCache.get(key) { path.openReadableChannel().readArrowFooter() }

        val arrowBlock = footer.recordBatches.getOrNull(idx)
            ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

        return memoryCache.get(
            PathSlice(cacheRootPath.resolve(key), arrowBlock.offset, arrowBlock.metadataLength + arrowBlock.bodyLength)
        ) { pathSlice ->
            memCacheMisses?.increment()
            val bufferCachePath =
                diskStore.resolve(cacheRootPath.relativize(pathSlice.path))
                    .takeIf { it.exists() } ?: throw objectMissingException(path)

            completedFuture(Pair(PathSlice(bufferCachePath, pathSlice.offset, pathSlice.length), null))
        }.use { arrowBuf ->
            arrowBuf.arrowBufToRecordBatch(
                0,
                arrowBlock.metadataLength,
                arrowBlock.bodyLength,
                "Failed opening record batch '$path' at block-idx $idx"
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
        } catch (_: ClosedByInterruptException) {
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

    override fun deleteIfExists(key: Path) {
        diskStore.resolve(key).deleteIfExists()
    }

    override fun openArrowWriter(key: Path, rel: Relation): ArrowWriter {
        val tmpPath = diskStore.createTempUploadFile()
        return newByteChannel(tmpPath, WRITE, TRUNCATE_EXISTING, CREATE).closeOnCatch { fileChannel ->
            rel.startUnload(fileChannel).closeOnCatch { unloader ->
                object : ArrowWriter {
                    override fun writePage() {
                        try {
                            unloader.writePage()
                        } catch (_: ClosedByInterruptException) {
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
        memoryCache.invalidate(PathSlice(cacheRootPath.resolve(key)))
    }

    override fun close() {
        allocator.close()
    }
}
