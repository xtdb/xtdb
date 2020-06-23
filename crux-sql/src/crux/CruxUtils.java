package crux.calcite;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Expression;

public class CruxUtils {
    private static IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    public static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

    public static IFn resolveWithErrorLogging(String symbolName) {
        IFn fn = (IFn) requiringResolve.invoke(Clojure.read(symbolName));
        IFn helper = (IFn) requiringResolve.invoke(Clojure.read("crux.calcite/clojure-helper-fn"));
        return (IFn) helper.invoke(fn);
    }

    public static Expression callExpression(java.lang.reflect.Method m, Expression[] args) {
        return Expressions.call(m, args);
    }
}
