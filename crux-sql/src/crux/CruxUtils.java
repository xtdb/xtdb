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
}
