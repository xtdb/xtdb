package xtdb.calcite;

import org.apache.calcite.linq4j.tree.Types;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.DataContext;

public enum XtdbMethod {
    XTDB_QUERYABLE_FIND(XtdbTable.XtdbQueryable.class, "find", Object.class, List.class, DataContext.class);

    public final Method method;
    public static final ImmutableMap<Method, XtdbMethod> MAP;

    static {
        final ImmutableMap.Builder<Method, XtdbMethod> builder = ImmutableMap.builder();
        for (XtdbMethod value : XtdbMethod.values()) {
            builder.put(value.method, value);
        }
        MAP = builder.build();
    }

    @SuppressWarnings("rawtypes")
    XtdbMethod(Class clazz, String methodName, Class... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }
}
