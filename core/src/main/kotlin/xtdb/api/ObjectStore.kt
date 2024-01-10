package xtdb.api

import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

interface ObjectStore {
    /**
     * Asynchronously returns the given object in a ByteBuffer. If the object doesn't exist,
     * the CompletableFuture completes with an IllegalStateException.
     */
    fun getObject(k: Path?): CompletableFuture<ByteBuffer?>?

    /**
     * Asynchronously returns a specified range of bytes from the object in a ByteBuffer.
     * If the object doesn't exist, the CompletableFuture completes with an IllegalStateException.
     */
    fun getObjectRange(k: Path?, start: Long, len: Long): CompletableFuture<ByteBuffer?>?

    /**
     * Asynchronously writes the object to the given path. If the object doesn't exist,
     * the CompletableFuture completes with an IllegalStateException.
     */
    fun getObject(k: Path?, outPath: Path?): CompletableFuture<Path?>?

    /**
     * Stores an object in the object store.
     */
    fun putObject(k: Path?, buf: ByteBuffer?): CompletableFuture<*>?

    /**
     * Lists all objects in the object store.
     */
    fun listObjects(): Iterable<Path?>?

    /**
     * Lists objects within a specified directory in the object store.
     */
    fun listObjects(dir: Path?): Iterable<Path?>?

    /**
     * Deletes the object with the given path from the object store.
     */
    fun deleteObject(k: Path?): CompletableFuture<*>?
}
