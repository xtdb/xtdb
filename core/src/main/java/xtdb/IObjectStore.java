package xtdb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public interface IObjectStore {
    /**
     * Asynchronously returns the given object in a ByteBuffer. If the object doesn't exist,
     * the CompletableFuture completes with an IllegalStateException.
     */
    CompletableFuture<ByteBuffer> getObject(Path k);

    /**
     * Asynchronously returns a specified range of bytes from the object in a ByteBuffer.
     * If the object doesn't exist, the CompletableFuture completes with an IllegalStateException.
     */
    CompletableFuture<ByteBuffer> getObjectRange(Path k, long start, long len);

    /**
     * Asynchronously writes the object to the given path. If the object doesn't exist,
     * the CompletableFuture completes with an IllegalStateException.
     */
    CompletableFuture<Path> getObject(Path k, Path outPath);

    /**
     * Stores an object in the object store.
     */
    CompletableFuture<?> putObject(Path k, ByteBuffer buf);

    /**
     * Lists all objects in the object store.
     */
    Iterable<Path> listObjects();

    /**
     * Lists objects within a specified directory in the object store.
     */
    Iterable<Path> listObjects(Path dir);

    /**
     * Deletes the object with the given path from the object store.
     */
    CompletableFuture<?> deleteObject(Path k);

}