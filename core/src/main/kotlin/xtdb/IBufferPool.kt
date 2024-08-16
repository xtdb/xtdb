package xtdb

import org.apache.arrow.memory.ArrowBuf
import xtdb.arrow.Relation
import java.nio.ByteBuffer
import java.nio.file.Path

interface IBufferPool : AutoCloseable {
    fun getBuffer(key: Path): ArrowBuf

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

    fun openArrowWriter(k: Path, rel: Relation): ArrowWriter
}
