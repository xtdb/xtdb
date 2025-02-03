package xtdb.api.storage

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.WRITE
import java.util.concurrent.CompletableFuture

interface ObjectStore : AutoCloseable {

    companion object {
        fun throwMissingKey(k: Path): Nothing = error("Object '$k' doesn't exist")
    }

    fun interface Factory {
        fun openObjectStore(): ObjectStore
    }

    data class StoredObject(val key: Path, val size: Long)

    /**
     * Asynchronously returns the given object in a ByteBuffer.
     *
     * If the object doesn't exist, the CompletableFuture completes with an IllegalStateException.
     */
    fun getObject(k: Path): CompletableFuture<ByteBuffer>

    /**
     * Asynchronously writes the object to the given path.
     *
     * If the object doesn't exist, the CompletableFuture completes with an IllegalStateException.
     */
    fun getObject(k: Path, outPath: Path): CompletableFuture<Path> =
        getObject(k).thenApply { buf ->
            FileChannel.open(outPath, CREATE, WRITE).use { it.write(buf) }
            outPath
        }

    /**
     * Stores an object in the object store.
     */
    fun putObject(k: Path, buf: ByteBuffer): CompletableFuture<Unit>

    /**
     * Recursively lists all objects in the object store.
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAllObjects(): Iterable<StoredObject>

    /**
     * Recursively lists all objects in the object store under the given directory.
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAllObjects(dir: Path): Iterable<StoredObject>

    /**
     * Deletes the object with the given path from the object store.
     */
    fun deleteObject(k: Path): CompletableFuture<Unit>

    override fun close() {
    }
}
