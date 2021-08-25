package crux.calcite;

import org.apache.calcite.linq4j.tree.Types;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function;

public enum CruxMethod {
    CRUX_QUERYABLE_FIND(CruxTable.CruxQueryable.class, "find", Object.class, List.class, DataContext.class);

    public final Method method;
    public static final ImmutableMap<Method, CruxMethod> MAP;

    static {
        final ImmutableMap.Builder<Method, CruxMethod> builder = ImmutableMap.builder();
        for (CruxMethod value : CruxMethod.values()) {
            builder.put(value.method, value);
        }
        MAP = builder.build();
    }

    @SuppressWarnings("rawtypes")
    CruxMethod(Class clazz, String methodName, Class... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }
}
