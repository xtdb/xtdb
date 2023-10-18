package xtdb;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;

public interface IBufferPool extends AutoCloseable {
    CompletableFuture<ArrowBuf> getBuffer(String key);

    CompletableFuture<?> putObject(String k, ByteBuffer buffer);

    Iterable<String> listObjects();

    Iterable<String> listObjects(String dir);

    WritableByteChannel openChannel(String k);

    ArrowFileWriter openArrowFileWriter(String k, VectorSchemaRoot vsr);
}
