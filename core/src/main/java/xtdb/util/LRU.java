package xtdb.util;

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.function.BiPredicate;

public class LRU<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = -4055957566679424062L;

    private final BiPredicate<LinkedHashMap<K, V>, Entry<K, V>> removeEldestEntryFn;

    public LRU(int size, BiPredicate<LinkedHashMap<K, V>, Entry<K, V>> removeEldestEntryFn) {
        super(size, 0.75f, true);
        this.removeEldestEntryFn = removeEldestEntryFn;
    }

    public boolean removeEldestEntry(Entry<K, V> entry) {
        return this.removeEldestEntryFn.test(this, entry);
    }
}
