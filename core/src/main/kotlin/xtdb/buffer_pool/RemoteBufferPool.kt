package xtdb.buffer_pool

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.slf4j.LoggerFactory
import xtdb.BufferPool
import xtdb.IEvictBufferTest
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.Storage.RemoteStorageFactory
import xtdb.api.storage.Storage.arrowFooterCache
import xtdb.api.storage.Storage.openStorageChildAllocator
import xtdb.api.storage.Storage.registerMetrics
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.ArrowUtil.openArrowBufView
import xtdb.arrow.ArrowUtil.readArrowFooter
import xtdb.arrow.ArrowUtil.toByteArray
import xtdb.arrow.Relation
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.cache.PathSlice
import xtdb.multipart.SupportsMultipart
import xtdb.trie.FileSize
import xtdb.util.closeOnCatch
import xtdb.util.maxDirectMemory
import xtdb.util.newSeekableByteChannel
import xtdb.util.toMmapPath
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.fileSize

class RemoteBufferPool(
    factory: RemoteStorageFactory,
    allocator: BufferAllocator,
    val objectStore: ObjectStore,
    meterRegistry: MeterRegistry,
) : BufferPool, IEvictBufferTest, Closeable {

    private val allocator = allocator.openStorageChildAllocator().also { it.registerMetrics(meterRegistry) }

    private val arrowFooterCache = arrowFooterCache()

    private val memoryCache =
        MemoryCache(allocator, factory.maxCacheBytes ?: (maxDirectMemory / 2))
            .also { it.registerMetrics("memory-cache", meterRegistry) }

    val diskCache = DiskCache(
        factory.localDiskCache.also { it.createDirectories() },
        factory.maxDiskCacheBytes
            ?: (factory.localDiskCache.totalSpace * (factory.maxDiskCachePercentage / 100.0).toLong())
    )

    private val recordBatchRequests: Counter = meterRegistry.counter("record-batch-requests")
    private val memCacheMisses: Counter = meterRegistry.counter("mem-cache-misses")
    private val diskCacheMisses: Counter = meterRegistry.counter("disk-cache-misses")

    companion object {
        internal var minMultipartPartSize = 5 * 1024 * 1024
        private const val MAX_CONCURRENT_PART_UPLOADS = 4

        private val Path.totalSpace get() = Files.getFileStore(this).totalSpace

        private val LOGGER = LoggerFactory.getLogger(RemoteBufferPool::class.java)

        private val multipartUploadDispatcher =
            Dispatchers.IO.limitedParallelism(MAX_CONCURRENT_PART_UPLOADS, "upload-multipart")

        @JvmStatic
        fun <P> SupportsMultipart<P>.uploadMultipartBuffers(key: Path, nioBuffers: List<ByteBuffer>) = runBlocking {
            val upload = startMultipart(key).await()

            try {
                val waitingParts = nioBuffers.map {
                    async(multipartUploadDispatcher) {
                        upload.uploadPart(it).await()
                    }
                }

                upload.complete(waitingParts.awaitAll()).await()
            } catch (e: Throwable) {
                try {
                    LOGGER.warn("Error caught in uploadMultipartBuffers - aborting multipart upload of $key")
                    upload.abort().get()
                } catch (abortError: Throwable) {
                    LOGGER.warn("Throwable caught when aborting uploadMultipartBuffers", abortError)
                    e.addSuppressed(abortError)
                }
                throw e
            }
        }

        private fun ArrowBuf.cuts(): List<Long> {
            val cuts = mutableListOf<Long>()
            var prevCut = 0L
            var cut = 0L

            for (block in this.readArrowFooter().recordBatches) {
                val offsetDelta = block.offset - cut
                val totalLength = offsetDelta + block.metadataLength + block.bodyLength
                val newCut = cut + totalLength
                val cutLen = newCut - prevCut

                if (cutLen >= minMultipartPartSize) {
                    cuts.add(newCut)
                    prevCut = newCut
                }

                cut = newCut
            }
            return cuts
        }

        private fun ArrowBuf.toParts(): List<ByteBuffer> {
            var prevCut = 0L
            val partBuffers = mutableListOf<ByteBuffer>()

            for (cut in cuts()) {
                partBuffers.add(nioBuffer(prevCut, (cut - prevCut).toInt()))
                prevCut = cut
            }

            return partBuffers.also { it.add(nioBuffer(prevCut, (capacity() - prevCut).toInt())) }
        }
    }

    private fun ObjectStore.uploadArrowFile(key: Path, tmpPath: Path) {
        val mmapBuffer = toMmapPath(tmpPath)

        if (this !is SupportsMultipart<*> || mmapBuffer.remaining() <= minMultipartPartSize) {
            putObject(key, mmapBuffer).get()
        } else {
            mmapBuffer.openArrowBufView(allocator).use { uploadMultipartBuffers(key, it.toParts()) }
        }
    }

    override fun getByteArray(key: Path): ByteArray =
        memoryCache.get(PathSlice(key)) { pathSlice ->
            memCacheMisses.increment()
            diskCache.get(key) { k, tmpFile ->
                diskCacheMisses.increment()
                objectStore.getObject(k, tmpFile)
            }.thenApply { entry -> Pair(PathSlice(entry.path, pathSlice.offset, pathSlice.length), entry) }
        }.use { it.toByteArray() }

    override fun getFooter(key: Path): ArrowFooter = arrowFooterCache.get(key) {
        diskCache
            .get(key) { k, tmpFile ->
                diskCacheMisses.increment()
                objectStore.getObject(k, tmpFile)
            }.get()
            .use { entry -> Relation.readFooter(entry.path.newSeekableByteChannel()) }
    }

    override fun getRecordBatch(key: Path, idx: Int): ArrowRecordBatch {
        recordBatchRequests.increment()

        val footer = getFooter(key)
        val arrowBlock = footer.recordBatches.getOrNull(idx)
            ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

        return memoryCache.get(PathSlice(key, arrowBlock.offset, arrowBlock.metadataLength + arrowBlock.bodyLength)) { pathSlice ->
            memCacheMisses.increment()
            diskCache.get(key) { k, tmpFile ->
                diskCacheMisses.increment()
                objectStore.getObject(k, tmpFile)
            }.thenApply { entry -> Pair(PathSlice(entry.path, pathSlice.offset, pathSlice.length), entry) }
        }.use { arrowBuf ->
            arrowBuf.arrowBufToRecordBatch(
                0, arrowBlock.metadataLength, arrowBlock.bodyLength,
                "Failed opening record batch '$key' at block-idx $idx"
            )
        }
    }

    override fun listAllObjects() = objectStore.listAllObjects()
    override fun listAllObjects(dir: Path) = objectStore.listAllObjects(dir)

    override fun openArrowWriter(key: Path, rel: Relation): xtdb.ArrowWriter {
        val tmpPath = diskCache.createTempPath()

        return FileChannel.open(tmpPath, READ, WRITE, TRUNCATE_EXISTING)
            .closeOnCatch { fileChannel ->
                rel.startUnload(fileChannel).closeOnCatch { unloader ->
                    object : xtdb.ArrowWriter {
                        override fun writePage() = unloader.writePage()

                        override fun end(): FileSize {
                            unloader.end()
                            fileChannel.close()

                            val size = tmpPath.fileSize()
                            objectStore.uploadArrowFile(key, tmpPath)

                            diskCache.put(key, tmpPath)
                            return size
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

    override fun putObject(key: Path, buffer: ByteBuffer) {
        objectStore.putObject(key, buffer).get()
    }

    override fun evictCachedBuffer(key: Path) {
        memoryCache.invalidate(PathSlice(key))
    }

    override fun close() {
        memoryCache.close()
        objectStore.close()
        allocator.close()
    }
}
