package xtdb.buffer_pool

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.IBufferPool
import xtdb.IEvictBufferTest
import xtdb.arrow.Relation
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.file.Path
import java.util.*
import xtdb.arrow.ArrowUtil
import xtdb.util.useAndCloseOnException

class MemoryBufferPool(
    private val allocator: BufferAllocator,
    private val memoryStore: NavigableMap<Path, ArrowBuf> = TreeMap()
) : IBufferPool, IEvictBufferTest {

    companion object {
        private fun objectMissingExcepiton(path: Path) = IllegalStateException("Object $path doesn't exist.")
    }

    override fun getByteArray(key: Path): ByteArray {
        val arrowBuf = synchronized(memoryStore) {
            memoryStore[key]
        } ?: throw objectMissingExcepiton(key)

        return ArrowUtil.arrowBufToByteArray(arrowBuf)
    }

    override fun getFooter(key: Path): ArrowFooter {
        val arrowBuf = synchronized(memoryStore) {
            memoryStore[key]
        } ?: throw objectMissingExcepiton(key)

        return ArrowUtil.readArrowFooter(arrowBuf)
    }

    override fun getRecordBatch(key: Path, blockIdx: Int): ArrowRecordBatch {
        try {
            val arrowBuf = synchronized(memoryStore) {
                memoryStore[key]
            } ?: throw objectMissingExcepiton(key)

            val footer = ArrowUtil.readArrowFooter(arrowBuf)
            val blocks = footer.recordBatches
            val block = blocks.getOrNull(blockIdx)
                ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

            return ArrowUtil.toArrowRecordBatchView(block, arrowBuf)
        } catch (e: Exception) {
            throw IllegalStateException("Failed opening record batch '$key'", e)
        }
    }

    override fun putObject(key: Path, buffer: ByteBuffer) {
        synchronized(memoryStore) {
            memoryStore[key] = ArrowUtil.toArrowBufView(allocator, buffer)
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
                .mapNotNull { path ->
                    if (path.nameCount > dirDepth) {
                        path.subpath(0, dirDepth + 1)
                    } else null
                }
                .distinct()
        }

    override fun objectSize(key: Path): Long = memoryStore[key]?.capacity() ?: 0

    override fun openArrowWriter(key: Path, rel: Relation): xtdb.ArrowWriter {
        val baos = ByteArrayOutputStream()
        return Channels.newChannel(baos).useAndCloseOnException { writeChannel ->
            rel.startUnload(writeChannel).useAndCloseOnException { unloader ->
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