package crux.calcite;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class CruxUtils {
    private static IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    public static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

}
