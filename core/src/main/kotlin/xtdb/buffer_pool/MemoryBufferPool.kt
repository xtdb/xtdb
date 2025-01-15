package xtdb.buffer_pool

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.IBufferPool
import xtdb.IEvictBufferTest
import xtdb.arrow.ArrowUtil.arrowBufToByteArray
import xtdb.arrow.ArrowUtil.readArrowFooter
import xtdb.arrow.ArrowUtil.toArrowBufView
import xtdb.arrow.ArrowUtil.toArrowRecordBatchView
import xtdb.arrow.Relation
import xtdb.util.closeOnCatch
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels.newChannel
import java.nio.file.Path
import java.util.*

class MemoryBufferPool(
    private val allocator: BufferAllocator,
    private val memoryStore: NavigableMap<Path, ArrowBuf> = TreeMap()
) : IBufferPool, IEvictBufferTest {

    companion object {
        private fun objectMissingException(path: Path) = IllegalStateException("Object $path doesn't exist.")

        private fun <K, V> Map<K, V>.lockAndGet(key: K): V? = synchronized(this) { get(key) }
    }

    override fun getByteArray(key: Path): ByteArray =
        arrowBufToByteArray(memoryStore.lockAndGet(key) ?: throw objectMissingException(key))

    override fun getFooter(key: Path): ArrowFooter =
        readArrowFooter(memoryStore.lockAndGet(key) ?: throw objectMissingException(key))

    override fun getRecordBatch(key: Path, blockIdx: Int): ArrowRecordBatch {
        try {
            val arrowBuf = memoryStore.lockAndGet(key) ?: throw objectMissingException(key)

            val block = readArrowFooter(arrowBuf).recordBatches.getOrNull(blockIdx)
                ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

            return toArrowRecordBatchView(block, arrowBuf)
        } catch (e: Exception) {
            throw IllegalStateException("Failed opening record batch '$key'", e)
        }
    }

    override fun putObject(key: Path, buffer: ByteBuffer) {
        synchronized(memoryStore) {
            memoryStore[key] = toArrowBufView(allocator, buffer)
        }
    }

    override fun listAllObjects(): List<Path> =
        synchronized(memoryStore) {
            memoryStore.keys.toList()
        }

    override fun listObjects(dir: Path): List<Path>  =
        synchronized(memoryStore) {
            val dirDepth = dir.nameCount
            memoryStore.tailMap(dir).keys
                .takeWhile { it.startsWith(dir) }
                .mapNotNull { path -> if (path.nameCount > dirDepth) path.subpath(0, dirDepth + 1) else null }
                .distinct()
        }

    override fun objectSize(key: Path): Long = memoryStore[key]?.capacity() ?: 0

    override fun openArrowWriter(key: Path, rel: Relation): xtdb.ArrowWriter {
        val baos = ByteArrayOutputStream()
        return newChannel(baos).closeOnCatch { writeChannel ->
            rel.startUnload(writeChannel).closeOnCatch { unloader ->
                object : xtdb.ArrowWriter {
                    override fun writeBatch() {
                        unloader.writeBatch()
                    }

                    override fun end() {
                        unloader.end()
                        writeChannel.close()
                        putObject(key, ByteBuffer.wrap(baos.toByteArray()))
                    }

                    override fun close() {
                        unloader.close()
                        if (writeChannel.isOpen) {
                            writeChannel.close()
                        }
                    }
                }
            }
        }
    }

    override fun evictCachedBuffer(key: Path) {}

    override fun close() {
        synchronized(memoryStore) {
            memoryStore.values.forEach { it.close() }
            memoryStore.clear()
        }
        allocator.close()
    }
}