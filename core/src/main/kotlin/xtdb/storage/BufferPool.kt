package xtdb.storage

import com.github.benmanes.caffeine.cache.Caffeine
import kotlinx.coroutines.runBlocking
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.ArrowWriter
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.arrow.Relation
import xtdb.arrow.unsupported
import xtdb.error.Fault
import xtdb.util.StringUtil.fromLexHex
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

typealias StorageEpoch = Int

interface BufferPool : AutoCloseable {

    val epoch: StorageEpoch

    /**
     * Get the whole file as an on-heap byte array.
     *
     * This should only be used for small files (metadata) or testing purposes.
     */
    fun getByteArray(key: Path): ByteArray

    /**
     * Returns the footer of the given arrow file.
     *
     * Throws if not an arrow file.
     */
    fun getFooter(key: Path): ArrowFooter

    /**
     * Returns a record batch of the given arrow file.
     *
     * Throws if not an arrow file or the record batch is out of bounds.
     */
    fun getRecordBatchSync(key: Path, idx: Int): ArrowRecordBatch = runBlocking { getRecordBatch(key, idx) }

    suspend fun getRecordBatch(key: Path, idx: Int): ArrowRecordBatch

    fun putObject(key: Path, buffer: ByteBuffer)

    /**
     * Recursively lists all objects in the buffer pool
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAllObjects(): Iterable<StoredObject>

    /**
     * Recursively lists all objects in the buffer pool, under the given directory.
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAllObjects(dir: Path): Iterable<StoredObject>

    fun copyObject(src: Path, dest: Path)

    fun deleteIfExists(key: Path)

    fun openArrowWriter(key: Path, rel: Relation): ArrowWriter

    companion object {
        @JvmField
        val UNUSED = UnusedBufferPool

        object UnusedBufferPool : BufferPool {
            override val epoch: StorageEpoch get() = unsupported("epoch")
            override fun getByteArray(key: Path) = unsupported("getByteArray")
            override fun getFooter(key: Path) = unsupported("getFooter")
            override suspend fun getRecordBatch(key: Path, idx: Int) = unsupported("getRecordBatch")
            override fun putObject(key: Path, buffer: ByteBuffer) = unsupported("putObject")
            override fun listAllObjects() = unsupported("listAllObjects")
            override fun listAllObjects(dir: Path) = unsupported("listAllObjects")
            override fun copyObject(src: Path, dest: Path) = unsupported("copyObject")
            override fun deleteIfExists(key: Path) = unsupported("deleteIfExists")
            override fun openArrowWriter(key: Path, rel: Relation) = unsupported("openArrowWriter")

            override fun close() = Unit
        }
    }

}

private fun Path.parseBlockIndex(): Long? =
    Regex("b(\\p{XDigit}+)\\.binpb")
        .matchEntire(toString())
        ?.groups?.get(1)
        ?.value?.fromLexHex

private val latestAvailableBlockCache =
    Caffeine.newBuilder().expireAfterWrite(1.minutes.toJavaDuration()).build<BufferPool, Long>()

val BufferPool.latestAvailableBlockIndex0: Long
    get() =
        listAllObjects("blocks".asPath)
            .lastOrNull()?.key?.fileName?.parseBlockIndex()
            ?: -1

val BufferPool.latestAvailableBlockIndex: Long
    get() = latestAvailableBlockCache.get(this) { it.latestAvailableBlockIndex0 }

/**
 * A wrapper around a BufferPool that prevents write operations.
 * Used for read-only database attachments.
 */
class ReadOnlyBufferPool(private val delegate: BufferPool) : BufferPool {
    override val epoch: StorageEpoch get() = delegate.epoch

    override fun getByteArray(key: Path): ByteArray = delegate.getByteArray(key)

    override fun getFooter(key: Path): ArrowFooter = delegate.getFooter(key)

    override suspend fun getRecordBatch(key: Path, idx: Int): ArrowRecordBatch = delegate.getRecordBatch(key, idx)

    override fun putObject(key: Path, buffer: ByteBuffer) {
        throw Fault("Attempted write to read-only storage: $key")
    }

    override fun listAllObjects(): Iterable<StoredObject> = delegate.listAllObjects()

    override fun listAllObjects(dir: Path): Iterable<StoredObject> = delegate.listAllObjects(dir)

    override fun copyObject(src: Path, dest: Path) {
        throw Fault("Attempted copy in read-only storage: $src -> $dest")
    }

    override fun deleteIfExists(key: Path) {
        throw Fault("Attempted delete from read-only storage: $key")
    }

    override fun openArrowWriter(key: Path, rel: Relation): ArrowWriter {
        throw Fault("Attempted write to read-only storage: $key")
    }

    override fun close() {
        delegate.close()
    }
}
