package xtdb.buffer_pool

import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.BufferPool
import xtdb.IEvictBufferTest
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.arrow.ArrowUtil.openArrowBufView
import xtdb.arrow.ArrowUtil.readArrowFooter
import xtdb.arrow.ArrowUtil.toArrowRecordBatchView
import xtdb.arrow.ArrowUtil.toByteArray
import xtdb.arrow.Relation
import xtdb.trie.FileSize
import xtdb.util.closeOnCatch
import xtdb.util.openChildAllocator
import xtdb.util.register
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels.newChannel
import java.nio.file.Path
import java.util.*

internal class MemoryBufferPool(
    allocator: BufferAllocator,
    meterRegistry: MeterRegistry? = null
) : BufferPool, IEvictBufferTest {

    private val allocator = allocator.openChildAllocator("buffer-pool").also { meterRegistry?.register(it) }

    private val memoryStore: NavigableMap<Path, ArrowBuf> = TreeMap()

    companion object {
        private fun objectMissingException(path: Path) = IllegalStateException("Object $path doesn't exist.")

        private fun <K, V> Map<K, V>.lockAndGet(key: K): V? = synchronized(this) { get(key) }
    }

    override fun getByteArray(key: Path): ByteArray =
        (memoryStore.lockAndGet(key) ?: throw objectMissingException(key)).toByteArray()

    override fun getFooter(key: Path): ArrowFooter =
        (memoryStore.lockAndGet(key) ?: throw objectMissingException(key)).readArrowFooter()

    override fun getRecordBatch(key: Path, idx: Int): ArrowRecordBatch {
        try {
            val arrowBuf = memoryStore.lockAndGet(key) ?: throw objectMissingException(key)

            val arrowBlock = arrowBuf.readArrowFooter().recordBatches.getOrNull(idx)
                ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

            return arrowBuf.toArrowRecordBatchView(arrowBlock)
        } catch (e: Exception) {
            throw IllegalStateException("Failed opening record batch '$key'", e)
        }
    }

    override fun putObject(key: Path, buffer: ByteBuffer) {
        synchronized(memoryStore) {
            memoryStore.put(key, buffer.openArrowBufView(allocator))?.close()
        }
    }

    override fun listAllObjects() =
        synchronized(memoryStore) {
            memoryStore.entries.map { StoredObject(it.key, it.value.capacity()) }
        }

    override fun listAllObjects(dir: Path) =
        synchronized(memoryStore) {
            memoryStore.tailMap(dir).entries
                .takeWhile { it.key.startsWith(dir) }
                .map { StoredObject(it.key, it.value.capacity()) }
        }

    override fun deleteAllObjects() {
        synchronized(memoryStore) {
            memoryStore.values.forEach { it.close() }
            memoryStore.clear()
        }
    }

    override fun deleteIfExists(key: Path): Unit =
        synchronized(memoryStore) {
            memoryStore.remove(key)?.also { it.close() }
        }

    override fun openArrowWriter(key: Path, rel: Relation): xtdb.ArrowWriter {
        val baos = ByteArrayOutputStream()
        return newChannel(baos).closeOnCatch { writeChannel ->
            rel.startUnload(writeChannel).closeOnCatch { unloader ->
                object : xtdb.ArrowWriter {
                    override fun writePage() {
                        unloader.writePage()
                    }

                    override fun end(): FileSize {
                        unloader.end()
                        writeChannel.close()
                        val bytes = baos.toByteArray()
                        putObject(key, ByteBuffer.wrap(bytes))
                        return bytes.size.toLong()
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
        deleteAllObjects()
        allocator.close()
    }
}
