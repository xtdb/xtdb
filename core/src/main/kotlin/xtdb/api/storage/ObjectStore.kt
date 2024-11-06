package xtdb.api.storage

import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

interface ObjectStore : AutoCloseable {

    interface Factory {
        fun openObjectStore(): ObjectStore
    }

    interface StoredObject {
        val key: Path
        val size: Long
    }

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
    fun getObject(k: Path, outPath: Path): CompletableFuture<Path>

    /**
     * Stores an object in the object store.
     */
    fun putObject(k: Path, buf: ByteBuffer): CompletableFuture<*>

    /**
     * Recursively lists all objects in the object store.
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAllObjects(): Iterable<StoredObject>

    /**
     * Deletes the object with the given path from the object store.
     */
    fun deleteObject(k: Path): CompletableFuture<*>

    override fun close() {
    }
}
