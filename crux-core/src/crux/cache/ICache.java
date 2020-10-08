package crux.cache;

import java.io.Closeable;
import java.util.Set;
import clojure.lang.Counted;
import clojure.lang.IFn;
import clojure.lang.ILookup;

public interface ICache<K, V> extends Closeable, Counted, ILookup {
    public V computeIfAbsent(K key, IFn storedKeyFn, IFn f);
    public void evict(K key);
    public Set<K> keySet();
}
