package xtdb

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.VectorSchemaRoot
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

interface IBufferPool : AutoCloseable {
    fun getBuffer(key: Path): CompletableFuture<ArrowBuf>

    fun putObject(k: Path, buffer: ByteBuffer): CompletableFuture<*>

    fun listAllObjects(): Iterable<Path>

    fun listObjects(dir: Path): Iterable<Path>

    fun openArrowWriter(k: Path, vsr: VectorSchemaRoot): IArrowWriter
}
