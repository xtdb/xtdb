package crux.cache.second_chance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.util.concurrent.ConcurrentHashMap;

public final class ConcurrentHashMapTableAccess {
    private static final MethodHandle TABLE_FIELD_METHOD_HANDLE;

    private static final Logger LOGGER =
        LoggerFactory.getLogger(ConcurrentHashMapTableAccess.class);

    static {
        try {
            Method getModule = Class.class.getDeclaredMethod("getModule");
            Class<?> moduleClass = getModule.getReturnType();
            Method isNamed = moduleClass.getDeclaredMethod("isNamed");
            Method addOpens = moduleClass.getDeclaredMethod("addOpens", String.class, moduleClass);

            Object thisModule = getModule.invoke(ConcurrentHashMapTableAccess.class);
            if (!(boolean) isNamed.invoke(thisModule)) {
                Object javaBaseModule = getModule.invoke(ConcurrentHashMap.class);
                addOpens.invoke(javaBaseModule, ConcurrentHashMap.class.getPackage().getName(), thisModule);
            }
        } catch (Exception ignore) {
        }

        MethodHandle mh = null;
        try {
            Field tableField = ConcurrentHashMap.class.getDeclaredField("table");
            tableField.setAccessible(true);
            mh = MethodHandles.lookup().unreflectGetter(tableField);
            mh = mh.asType(mh.type().changeReturnType(Object[].class));
        } catch (Exception e) {
            LOGGER.warn("Could not open ConcurrentHashMap.table field - Crux's cache may perform badly in certain cases. Use `--add-opens java.base/java.util.concurrent=ALL-UNNAMED` to remove this warning.");
        }

        TABLE_FIELD_METHOD_HANDLE = mh;
    }

    public static final <K, V> Object[] getConcurrentHashMapTable(final ConcurrentHashMap<K, V> map) throws Throwable {
        if (TABLE_FIELD_METHOD_HANDLE == null) return null;
        return (Object[]) TABLE_FIELD_METHOD_HANDLE.invokeExact(map);
    }
}
