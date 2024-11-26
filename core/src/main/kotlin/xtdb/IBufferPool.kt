package xtdb

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.arrow.Relation
import java.nio.ByteBuffer
import java.nio.file.Path

interface IBufferPool : AutoCloseable {
    fun getBuffer(key: Path): ArrowBuf

    /**
     * Get the whole file as an on-heap ByteBuffer.
     *
     * This should only be used for small files (metadata) or testing purposes.
     */
    fun getByteBuffer(key: Path): ByteBuffer

    /**
     * Returns the footer of the given arrow file.
     *
     * Throws if not an arrow file.
     */
    fun getFooter (key: Path): ArrowFooter

    /**
     * Returns a record batch of the given arrow file.
     *
     * Throws if not an arrow file or the record batch is out of bounds.
     */
    fun getRecordBatch(key: Path, blockIdx: Int): ArrowRecordBatch

    fun putObject(k: Path, buffer: ByteBuffer)

    /**
     * Recursively lists all objects in the buffer pool.
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAllObjects(): Iterable<Path>

    /**
     * Lists objects directly within the specified directory in the buffer pool.
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listObjects(dir: Path): Iterable<Path>

    fun objectSize(k: Path): Long

    fun openArrowWriter(k: Path, rel: Relation): ArrowWriter
}
