package crux.calcite;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class CruxUtils {
    private static IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    public static IFn resolve(String symbolName) {
        IFn fn = (IFn) requiringResolve.invoke(Clojure.read(symbolName));
        IFn helper = (IFn) requiringResolve.invoke(Clojure.read("crux.calcite/clojure-helper-fn"));
        return (IFn) helper.invoke(fn);
    }

    public static Object invokeStaticMethod(java.lang.reflect.Method m, Object[] args) {
	try {
            Object[] boxedArgs = boxArgs(m.getParameterTypes(), args);
            return m.invoke(null, boxedArgs);
        }
	catch(Exception e) {
            throw clojure.lang.Util.sneakyThrow(getCauseOrElse(e));
        }
    }

   // Taken from clojure.lang.Reflector:
    static Object[] boxArgs(Class<?>[] params, Object[] args){
	if(params.length == 0)
            return null;
	Object[] ret = new Object[params.length];
	for(int i = 0; i < params.length; i++)
            {
		Object arg = args[i];
		Class<?> paramType = params[i];
		ret[i] = boxArg(paramType, arg);
            }
	return ret;
    }

    // Taken from clojure.lang.Reflector:
    static Object boxArg(Class<?> paramType, Object arg){
	if(!paramType.isPrimitive())
            return paramType.cast(arg);
	else if(paramType == boolean.class)
            return Boolean.class.cast(arg);
	else if(paramType == char.class)
            return Character.class.cast(arg);
	else if(arg instanceof Number)
            {
		Number n = (Number) arg;
		if(paramType == int.class)
                    return n.intValue();
		else if(paramType == float.class)
                    return n.floatValue();
		else if(paramType == double.class)
                    return n.doubleValue();
		else if(paramType == long.class)
                    return n.longValue();
		else if(paramType == short.class)
                    return n.shortValue();
		else if(paramType == byte.class)
                    return n.byteValue();
            }
	throw new IllegalArgumentException("Unexpected param type, expected: " + paramType +
	                                   ", given: " + arg.getClass().getName());
    }

    private static Throwable getCauseOrElse(Exception e) {
	if (e.getCause() != null)
            return e.getCause();
	return e;
    }
}
