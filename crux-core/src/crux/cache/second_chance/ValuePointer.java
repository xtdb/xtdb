package crux.cache.second_chance;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

import java.util.concurrent.ConcurrentHashMap;

public final class ValuePointer<K, V> {
    private static final MethodHandle TABLE_FIELD_METHOD_HANDLE;

    static {
        try {
            Field tableField = ConcurrentHashMap.class.getDeclaredField("table");
            tableField.setAccessible(true);
            TABLE_FIELD_METHOD_HANDLE = MethodHandles.lookup().unreflectGetter(tableField);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final <K, V> Object[] getConcurrentHashMapTable(final ConcurrentHashMap<K, V> map) {
        try {
            return (Object[]) TABLE_FIELD_METHOD_HANDLE.invoke(map);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public K coldKey;
    public final V value;

    public ValuePointer(final V value) {
        this.value = value;
    }
}
