package xtdb.util;

import org.apache.arrow.memory.ArrowBuf;

import java.util.LinkedHashMap;
import java.util.Map;

public class ArrowBufLRU<K> extends LinkedHashMap<K, ArrowBuf> {
    private static final long serialVersionUID = 1L;

    private long byteSize;
    private final int maxSize;
    private final long maxByteSize;

    @Override
    public ArrowBuf put(K key, ArrowBuf buf) {
        byteSize += buf.capacity();
        return super.put(key, buf);
    }

    public ArrowBufLRU(int initialSize, int maxSize, long maxByteSize) {
        super(initialSize, 0.75f, true);
        this.byteSize = 0;
        this.maxSize = maxSize;
        this.maxByteSize = maxByteSize;
    }

    public boolean removeEldestEntry(Map.Entry<K, ArrowBuf> entry) {
        if(maxSize < size() || maxByteSize < byteSize){
            byteSize -= entry.getValue().capacity();
            entry.getValue().close();
            return true;
        }
        return false;
    }
}
