package crux.cache.second_chance;

public final class ValuePointer<K, V> {
    private K coolingKey;
    private final V value;

    public ValuePointer(final V value) {
        this.value = value;
    }

    public final K getKey() {
        return this.coolingKey;
    }

    public final V swizzle() {
        this.coolingKey = null;
        return this.value;
    }

    public final void unswizzle(K key) {
        this.coolingKey = key;
    }

    public final boolean isSwizzled() {
        return this.coolingKey == null;
    }
}
