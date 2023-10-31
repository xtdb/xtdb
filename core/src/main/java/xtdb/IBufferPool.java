package xtdb;

import org.apache.arrow.memory.ArrowBuf;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface IBufferPool extends AutoCloseable {
    CompletableFuture<ArrowBuf> getBuffer(String key);

    CompletableFuture<?> putObject(String k, ByteBuffer buffer);

    Iterable<String> listObjects();

    Iterable<String> listObjects(String dir);
}
