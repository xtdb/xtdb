package xtdb.buffer_pool

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.slf4j.LoggerFactory
import xtdb.IBufferPool
import xtdb.IEvictBufferTest
import xtdb.api.log.FileLog
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
import xtdb.util.*
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.fileSize

class RemoteBufferPool(
    factory: RemoteStorageFactory,
    allocator: BufferAllocator,
    private val fileLog: FileLog,
    val objectStore: ObjectStore,
    meterRegistry: MeterRegistry,
) : IBufferPool, IEvictBufferTest, Closeable {

    private val allocator = allocator.openStorageChildAllocator().also { it.registerMetrics(meterRegistry) }

    private val arrowFooterCache = arrowFooterCache()
    val osFiles: SortedMap<Path, Long> = TreeMap()

    private val osFilesSubscription = fileLog.subscribeFileNotifications { msg ->
        osFiles.putAll(msg.added.associate { it.key to it.size })
    }

    init {
        // must come after the subscription is set up - at _least_ once processing.
        osFiles.putAll(objectStore.listAllObjects().associate { it.key to it.size })
    }

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
        private const val MAX_MULTIPART_PER_UPLOAD_CONCURRENCY = 4

        private val Path.totalSpace get() = Files.getFileStore(this).totalSpace

        private val FileNotificationAddition = requiringResolve("xtdb.file-log/addition")

        private fun additionNotification(key: Path, size: Long) =
            FileNotificationAddition(key, size) as FileLog.Notification

        private val LOGGER = LoggerFactory.getLogger(RemoteBufferPool::class.java)

        @JvmStatic
        fun ObjectStore.uploadMultipartBuffers(key: Path, nioBuffers: List<ByteBuffer>) {
            val upload = (this as SupportsMultipart).startMultipart(key).get()

            try {
                val partQueue = ArrayDeque(nioBuffers)
                val waitingParts = mutableListOf<CompletableFuture<*>>()

                while (partQueue.isNotEmpty()) {
                    if (waitingParts.size < MAX_MULTIPART_PER_UPLOAD_CONCURRENCY) {
                        waitingParts.add(upload.uploadPart(partQueue.removeFirst()))
                    } else {
                        CompletableFuture.anyOf(*waitingParts.toTypedArray()).get()
                        waitingParts.removeAll { it.isDone }
                    }
                }

                CompletableFuture.allOf(*waitingParts.toTypedArray()).get()
                upload.complete().get()
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

        private fun SortedMap<Path, Long>.listFilesUnderPrefix(prefix: Path): List<Path> {
            val prefixDepth = prefix.nameCount
            return tailMap(prefix).keys
                .asSequence()
                .takeWhile { it.startsWith(prefix) }
                .mapNotNull { if (it.nameCount > prefixDepth) it.subpath(0, prefixDepth + 1) else null }
                .distinct().toList()
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

        if (this !is SupportsMultipart || mmapBuffer.remaining() <= minMultipartPartSize) {
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

    override fun getRecordBatch(key: Path, blockIdx: Int): ArrowRecordBatch {
        recordBatchRequests.increment()

        val footer = getFooter(key)
        val block = footer.recordBatches.getOrNull(blockIdx)
            ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

        return memoryCache.get(PathSlice(key, block.offset, block.metadataLength + block.bodyLength)) { pathSlice ->
            memCacheMisses.increment()
            diskCache.get(key) { k, tmpFile ->
                diskCacheMisses.increment()
                objectStore.getObject(k, tmpFile)
            }.thenApply { entry -> Pair(PathSlice(entry.path, pathSlice.offset, pathSlice.length), entry) }
        }.use { arrowBuf ->
            arrowBuf.arrowBufToRecordBatch(
                0, block.metadataLength, block.bodyLength, "Failed opening record batch '$key' at block-idx $blockIdx"
            )
        }
    }

    override fun listAllObjects(): List<Path> = osFiles.keys.toList()

    override fun listObjects(dir: Path): List<Path> = osFiles.listFilesUnderPrefix(dir)

    override fun objectSize(key: Path): Long = osFiles[key] ?: 0

    override fun openArrowWriter(key: Path, rel: Relation): xtdb.ArrowWriter {
        val tmpPath = diskCache.createTempPath()
        return FileChannel.open(tmpPath, READ, WRITE, TRUNCATE_EXISTING).closeOnCatch { fileChannel ->
            rel.startUnload(fileChannel).closeOnCatch { unloader ->
                object : xtdb.ArrowWriter {
                    override fun writeBatch() = unloader.writeBatch()

                    override fun end() {
                        unloader.end()
                        fileChannel.close()

                        objectStore.uploadArrowFile(key, tmpPath)

                        fileLog.appendFileNotification(additionNotification(key, tmpPath.fileSize()))

                        diskCache.put(key, tmpPath)
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
        objectStore.putObject(key, buffer)
            .thenApply { fileLog.appendFileNotification(additionNotification(key, buffer.capacity().toLong())) }
            .get()
    }

    override fun evictCachedBuffer(key: Path) {
        memoryCache.invalidate(PathSlice(key))
    }

    override fun close() {
        memoryCache.close()
        osFilesSubscription.close()
        objectStore.close()
        allocator.close()
    }
}
