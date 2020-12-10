package crux.cache.second_chance;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.util.concurrent.ConcurrentHashMap;

import crux.ByteUtils;

public final class ConcurrentHashMapTableAccess {
    private static final MethodHandle TABLE_FIELD_METHOD_HANDLE;

    static {
        try {
            ByteUtils.tryOpenReflectiveAccess(ConcurrentHashMapTableAccess.class, ConcurrentHashMap.class);
            Field tableField = ConcurrentHashMap.class.getDeclaredField("table");
            tableField.setAccessible(true);
            MethodHandle mh = MethodHandles.lookup().unreflectGetter(tableField);
            TABLE_FIELD_METHOD_HANDLE = mh.asType(mh.type().changeReturnType(Object[].class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final <K, V> Object[] getConcurrentHashMapTable(final ConcurrentHashMap<K, V> map) throws Throwable {
        return (Object[]) TABLE_FIELD_METHOD_HANDLE.invokeExact(map);
    }
}
