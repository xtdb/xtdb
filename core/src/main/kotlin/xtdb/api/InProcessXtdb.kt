package xtdb.api

import clojure.java.api.Clojure
import clojure.lang.IFn
import clojure.lang.Symbol

object InProcessXtdb {
    init {
        Clojure.`var`("clojure.core", "require").invoke(Symbol.intern("xtdb.node"))
    }

    private val START_NODE: IFn = Clojure.`var`("xtdb.node", "start-node")

    @JvmStatic
    fun startNode(): IXtdb {
        return START_NODE.invoke() as IXtdb
    }
}
