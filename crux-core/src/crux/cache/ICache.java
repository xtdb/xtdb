package crux.cache;

import clojure.lang.Counted;
import clojure.lang.IFn;
import clojure.lang.ILookup;

public interface ICache<K, V> extends ILookup, Counted {
    public V computeIfAbsent(K key, IFn storedKeyFn, IFn f);
    public void evict(K key);
}
