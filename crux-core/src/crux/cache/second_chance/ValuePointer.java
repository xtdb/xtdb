package crux.cache.second_chance;

public final class ValuePointer<K, V> {
    public K coldKey;
    public final V value;

    public ValuePointer(V value) {
        this.value = value;
    }
}
