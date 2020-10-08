package crux.cache.soft_values;

import java.lang.ref.SoftReference;
import java.lang.ref.ReferenceQueue;

public final class SoftReferenceWithKey<K, V> extends SoftReference<V> {
    public final K key;

    public SoftReferenceWithKey(K key, V referent, ReferenceQueue<V> referenceQueue) {
        super(referent, referenceQueue);
        this.key = key;
    }

    public SoftReferenceWithKey(K key, V referent) {
        super(referent);
        this.key = key;
    }
}
