package xtdb;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Symbol;
import xtdb.api.IXtdb;

public class InProcessXtdb {
    static {
        Clojure.var("clojure.core", "require").invoke(Symbol.intern("xtdb.node"));
    }

    private static final IFn START_NODE = Clojure.var("xtdb.node", "start-node");

    public static IXtdb startNode() {
        return (IXtdb) START_NODE.invoke();
    }

    private InProcessXtdb() {

    }
}
