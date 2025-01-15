package xtdb.buffer_pool

import com.github.benmanes.caffeine.cache.Cache
import io.micrometer.core.instrument.Counter
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.slf4j.LoggerFactory
import xtdb.IBufferPool
import xtdb.IEvictBufferTest
import xtdb.api.log.FileListCache
import xtdb.api.storage.ObjectStore
import xtdb.arrow.ArrowUtil
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.ArrowUtil.readArrowFooter
import xtdb.arrow.ArrowUtil.toArrowBufView
import xtdb.arrow.Relation
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.cache.PathSlice
import xtdb.multipart.SupportsMultipart
import xtdb.util.requiringResolve
import xtdb.util.toMmapPath
import xtdb.util.closeOnCatch
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.*
import java.util.concurrent.CompletableFuture

class RemoteBufferPool(
    private val allocator: BufferAllocator,
    private val memoryCache: MemoryCache,
    val diskCache: DiskCache,
    private val arrowFooterCache: Cache<Path, ArrowFooter>,
    private val fileListCache: FileListCache,
    val objectStore: ObjectStore,
    val osFiles: SortedMap<Path, Long>,
    private val osFilesSubscription: AutoCloseable,
    private val recordBatchRequests: Counter,
    private val memCacheMisses: Counter,
    private val diskCacheMisses: Counter
) : IBufferPool, IEvictBufferTest, Closeable {

    companion object {
        private var minMultipartPartSize = 5 * 1024 * 1024
        private const val MAX_MULTIPART_PER_UPLOAD_CONCURRENCY = 4

        internal fun setMinMultipartPartSize(size: Int) {
            minMultipartPartSize = size
        }

        private val FileNotificationAddition = requiringResolve("xtdb.file-list-cache/addition")

        private fun pathToSeekableByteChannel(path: Path) = SeekableReadChannel(Files.newByteChannel(path, READ))

        private val LOGGER = LoggerFactory.getLogger(RemoteBufferPool::class.java)

        private fun listFilesUnderPrefix(files: SortedMap<Path, Long>, prefix: Path): List<Path> {
            val prefixDepth = prefix.nameCount
            return files.tailMap(prefix).keys
                .asSequence()
                .takeWhile { it.startsWith(prefix) }
                .mapNotNull { path -> if (path.nameCount > prefixDepth) path.subpath(0, prefixDepth + 1) else null }
                .distinct()
                .toList()
        }

        @JvmStatic
        fun uploadMultipartBuffers(objectStore: ObjectStore, key: Path, nioBuffers: List<ByteBuffer>) {
            val upload = (objectStore as SupportsMultipart).startMultipart(key).get()

            try {
                val partQueue = ArrayDeque(nioBuffers)
                val waitingParts = mutableListOf<CompletableFuture<*>>()

                while (partQueue.isNotEmpty()) {
                    if (waitingParts.size < MAX_MULTIPART_PER_UPLOAD_CONCURRENCY) {
                        val buffer = partQueue.removeFirst()
                        waitingParts.add(upload.uploadPart(buffer))
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

        private fun arrowBufCuts(arrowBuf: ArrowBuf): List<Long> {
            val cuts = mutableListOf<Long>()
            var prevCut = 0L
            var cut = 0L

            val blocks = readArrowFooter(arrowBuf).recordBatches
            for (block in blocks) {
                val offset = block.offset
                val offsetDelta = offset - cut
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

        private fun arrowBufToParts(arrowBuf: ArrowBuf): List<ByteBuffer> {
            var prevCut = 0L
            val cuts = arrowBufCuts(arrowBuf)
            val partBuffers = mutableListOf<ByteBuffer>()

            for (cut in cuts) {
                partBuffers.add(arrowBuf.nioBuffer(prevCut, (cut - prevCut).toInt()))
                prevCut = cut
            }

            val finalPart = arrowBuf.nioBuffer(prevCut, (arrowBuf.capacity() - prevCut).toInt())
            partBuffers.add(finalPart)
            return partBuffers
        }

        private fun uploadArrowFile(allocator: BufferAllocator, objectStore: ObjectStore, key: Path, tmpPath: Path) {
            val mmapBuffer = toMmapPath(tmpPath)

            if (objectStore !is SupportsMultipart || mmapBuffer.remaining() <= minMultipartPartSize) {
                objectStore.putObject(key, mmapBuffer).get()
            } else {
                toArrowBufView(allocator, mmapBuffer).use { arrowBuf ->
                    uploadMultipartBuffers(objectStore, key, arrowBufToParts(arrowBuf))
                }
            }
        }
    }

    override fun getByteArray(key: Path): ByteArray =
        memoryCache.get(PathSlice(key)) { pathSlice ->
            memCacheMisses.increment()
            diskCache.get(key) { k, tmpFile ->
                diskCacheMisses.increment()
                objectStore.getObject(k, tmpFile)
            }.thenApply { entry -> Pair(PathSlice(entry.path, pathSlice.offset, pathSlice.length), entry) }
        }.use(ArrowUtil::arrowBufToByteArray)

    override fun getFooter(key: Path): ArrowFooter = arrowFooterCache.get(key) {
        diskCache
            .get(key) { k, tmpFile ->
                diskCacheMisses.increment()
                objectStore.getObject(k, tmpFile)
            }.get()
            .use { entry -> Relation.readFooter(pathToSeekableByteChannel(entry.path)) }
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
            arrowBufToRecordBatch(
                arrowBuf, 0, block.metadataLength, block.bodyLength,
                "Failed opening record batch '$key' at block-idx $blockIdx"
            )
        }
    }

    override fun listAllObjects(): List<Path> = osFiles.keys.toList()

    override fun listObjects(dir: Path): List<Path> = listFilesUnderPrefix(osFiles, dir)

    override fun objectSize(key: Path): Long = osFiles[key] ?: 0

    override fun openArrowWriter(key: Path, rel: Relation): xtdb.ArrowWriter {
        val tmpPath = diskCache.createTempPath()
        return FileChannel.open(tmpPath, READ, WRITE, TRUNCATE_EXISTING).closeOnCatch { fileChannel ->
            rel.startUnload(fileChannel).closeOnCatch { unloader ->
                object : xtdb.ArrowWriter {
                    override fun writeBatch() {
                        unloader.writeBatch()
                    }

                    override fun end() {
                        unloader.end()
                        fileChannel.close()

                        uploadArrowFile(allocator, objectStore, key, tmpPath)

                        fileListCache.appendFileNotification(
                            FileNotificationAddition.invoke(key, tmpPath) as FileListCache.Notification
                        )

                        diskCache.put(key, tmpPath)
                    }

                    override fun close() {
                        unloader.close()
                        if (fileChannel.isOpen) fileChannel.close()
                        Files.deleteIfExists(tmpPath)
                    }
                }
            }
        }
    }

    override fun putObject(key: Path, buffer: ByteBuffer) {
        objectStore.putObject(key, buffer)
            .thenApply {
                fileListCache.appendFileNotification(
                    FileNotificationAddition.invoke(key, buffer.capacity()) as FileListCache.Notification
                )
            }.get()
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