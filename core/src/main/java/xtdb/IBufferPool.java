package xtdb;

import org.apache.arrow.memory.ArrowBuf;

import java.util.concurrent.CompletableFuture;

public interface IBufferPool {
    CompletableFuture<ArrowBuf> getBuffer(String key);
    CompletableFuture<ArrowBuf> getRangeBuffer(String key, int start, int len);
    boolean evictBuffer(String key);
}
