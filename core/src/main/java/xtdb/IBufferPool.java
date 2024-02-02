package xtdb;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public interface IBufferPool extends AutoCloseable {
    CompletableFuture<ArrowBuf> getBuffer(Path key);

    CompletableFuture<?> putObject(Path k, ByteBuffer buffer);

    Iterable<Path> listAllObjects();

    Iterable<Path> listObjects(Path dir);

    IArrowWriter openArrowWriter(Path k, VectorSchemaRoot vsr);
}
