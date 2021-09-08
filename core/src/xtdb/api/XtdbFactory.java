package xtdb.api;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

class XtdbFactory {
    private static final IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

    private XtdbFactory() { }

    static IXtdb startNode(Object config) {
        Object xtdbNode = resolve("xtdb.api/start-node").invoke(config);
        return (IXtdb) resolve("xtdb.api.java/->JXtdbNode").invoke(xtdbNode);
    }
}
