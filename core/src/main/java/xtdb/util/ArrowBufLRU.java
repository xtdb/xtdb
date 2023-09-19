package xtdb.util;

import org.apache.arrow.memory.ArrowBuf;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiPredicate;

public class ArrowBufLRU<K, ArrowBuf> extends LinkedHashMap<K, ArrowBuf> {
    private static final long serialVersionUID = 1L;

    private long byteSize;
    private final BiPredicate<Map<K, ArrowBuf>, Entry<K, ArrowBuf>> removeEldestEntryFn;

    @Override
    public ArrowBuf put(K key, ArrowBuf buf) {
        byteSize += ((org.apache.arrow.memory.ArrowBuf) buf).capacity();
        return super.put(key, (ArrowBuf) buf);
    }

    public ArrowBufLRU(int initialSize, int maxSize, long maxByteSize) {
        super(initialSize, 0.75f, true);
        this.byteSize = 0;
        this.removeEldestEntryFn = new BiPredicate<Map<K, ArrowBuf>, Entry<K, ArrowBuf>>() {
            @Override
            public boolean test(Map<K, ArrowBuf> m, Entry<K, ArrowBuf> entry) {
                if(maxSize < m.size() || maxByteSize < byteSize){
                    ((org.apache.arrow.memory.ArrowBuf) entry.getValue()).close();
                    return  true;
                }
                return false;
            }
        };
    }

    public boolean removeEldestEntry(Map.Entry<K, ArrowBuf> entry) {
        var res = this.removeEldestEntryFn.test(this, entry);
        if (res) byteSize -= ((org.apache.arrow.memory.ArrowBuf) entry.getValue()).capacity();
        return res;
    }
}
