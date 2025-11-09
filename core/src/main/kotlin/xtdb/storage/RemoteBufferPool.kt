package xtdb.storage

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.ArrowWriter
import xtdb.IEvictBufferTest
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.Storage.arrowFooterCache
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.ArrowUtil.openArrowBufView
import xtdb.arrow.ArrowUtil.readArrowFooter
import xtdb.arrow.ArrowUtil.toByteArray
import xtdb.arrow.ArrowUtil.toByteBuffer
import xtdb.arrow.BufferedWritableByteChannel
import xtdb.arrow.Relation
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.database.DatabaseName
import xtdb.multipart.SupportsMultipart
import xtdb.multipart.SupportsMultipart.Companion.uploadMultipartBuffers
import xtdb.trie.FileSize
import xtdb.util.*
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import kotlin.io.path.deleteIfExists
import kotlin.io.path.fileSize

internal class RemoteBufferPool(
    allocator: BufferAllocator,
    val objectStore: ObjectStore,
    val memoryCache: MemoryCache,
    val diskCache: DiskCache,
    meterRegistry: MeterRegistry? = null,
    override val epoch: StorageEpoch,
    dbName: DatabaseName,
) : BufferPool, IEvictBufferTest, Closeable {

    private val allocator = allocator.openChildAllocator("buffer-pool").also { meterRegistry?.register(it) }

    private val arrowFooterCache = arrowFooterCache()

    private val recordBatchRequests: Counter? = meterRegistry?.counter("record-batch-requests")
    private val memCacheMisses: Counter? = meterRegistry?.counter("memory-cache-misses")
    private val diskCacheMisses: Counter? = meterRegistry?.counter("disk-cache-misses")
    private val networkWrite: Counter? = meterRegistry?.counter("buffer-pool.network.write")
    private val networkRead: Counter? = meterRegistry?.counter("buffer-pool.network.read")

    // we partition the caches by dbName as they're shared between multiple databases
    // the caches themselves has no knowledge of this
    // '0' for partition 0, in advance of multi-partition support
    private val cacheRootPath = dbName.asPath.resolve("0")

    companion object {
        internal var minMultipartPartSize = 5 * 1024 * 1024

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
                partBuffers.add(toByteBuffer(prevCut, cut - prevCut))
                prevCut = cut
            }

            return partBuffers.also { it.add(toByteBuffer(prevCut, capacity() - prevCut)) }
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

    private fun getObject(key: Path, tmpFile: Path) =
        objectStore.getObject(key, tmpFile).thenApply { path ->
            networkRead?.increment(path.fileSize().toDouble())
            path
        }

    override fun getByteArray(key: Path): ByteArray = runBlocking {
        memoryCache.get(cacheRootPath.resolve(key)) { path ->
            memCacheMisses?.increment()
            diskCache.get(path) { k, tmpFile ->
                diskCacheMisses?.increment()
                getObject(cacheRootPath.relativize(k), tmpFile)
            }.thenApply { entry -> Pair(entry.path, entry) }.await()
        }.use { it.toByteArray() }
    }

    override fun getFooter(key: Path): ArrowFooter = arrowFooterCache.get(key) {
        diskCache
            .get(cacheRootPath.resolve(key)) { k, tmpFile ->
                diskCacheMisses?.increment()
                getObject(cacheRootPath.relativize(k), tmpFile)
            }.get()
            .use { entry -> entry.path.openReadableChannel().readArrowFooter() }
    }

    override suspend fun getRecordBatch(key: Path, idx: Int): ArrowRecordBatch {
        recordBatchRequests?.increment()

        val footer = getFooter(key)
        val arrowBlock = footer.recordBatches.getOrNull(idx)
            ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

        return memoryCache.get(
            cacheRootPath.resolve(key),
            MemoryCache.Slice(arrowBlock.offset, arrowBlock.metadataLength + arrowBlock.bodyLength)
        ) { path ->
            memCacheMisses?.increment()
            diskCache.get(path) { k, tmpFile ->
                diskCacheMisses?.increment()
                getObject(cacheRootPath.relativize(k), tmpFile)
            }.thenApply { entry -> Pair(entry.path, entry) }.await()
        }.use { arrowBuf ->
            arrowBuf.arrowBufToRecordBatch(
                0, arrowBlock.metadataLength, arrowBlock.bodyLength,
                "Failed opening record batch '$key' at block-idx $idx"
            )
        }
    }

    override fun listAllObjects() = objectStore.listAllObjects()
    override fun listAllObjects(dir: Path) = objectStore.listAllObjects(dir)
    override fun copyObject(src: Path, dest: Path): Unit = objectStore.copyObject(src, dest).get()

    override fun deleteIfExists(key: Path): Unit = runBlocking { objectStore.deleteIfExists(key).await() }

    override fun openArrowWriter(key: Path, rel: Relation): ArrowWriter {
        val tmpPath = diskCache.createTempPath()

        return FileChannel.open(tmpPath, READ, WRITE, TRUNCATE_EXISTING)
            .closeOnCatch { fileChannel ->
                BufferedWritableByteChannel(fileChannel).closeOnCatch { bufferedCh ->
                    rel.startUnload(bufferedCh).closeOnCatch { unloader ->
                        object : ArrowWriter {
                            override fun writePage() = unloader.writePage()

                            override fun end(): FileSize {
                                unloader.end()
                                bufferedCh.close()

                                val size = tmpPath.fileSize()
                                objectStore.uploadArrowFile(key, tmpPath)
                                networkWrite?.increment(size.toDouble())

                                diskCache.put(key, tmpPath)
                                return size
                            }

                            override fun close() {
                                unloader.close()
                                if (bufferedCh.isOpen) bufferedCh.close()
                                tmpPath.deleteIfExists()
                            }
                        }
                    }
                }
            }
    }

    override fun putObject(key: Path, buffer: ByteBuffer) {
        networkWrite?.increment(buffer.capacity().toDouble())
        objectStore.putObject(key, buffer).get()
    }

    override fun close() {
        objectStore.close()
        allocator.close()
    }
}
