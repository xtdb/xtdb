package xtdb

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.VectorSchemaRoot
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

interface IBufferPool : AutoCloseable {
    fun getBuffer(key: Path): ArrowBuf

    fun putObject(k: Path, buffer: ByteBuffer): CompletableFuture<*>

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

    fun openArrowWriter(k: Path, vsr: VectorSchemaRoot): IArrowWriter
}
