package xtdb.api

import clojure.java.api.Clojure
import clojure.lang.IFn
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap
import clojure.lang.Symbol

private val String.kw: Keyword get() = Keyword.intern(this)

object XtdbServer {
    init {
        Clojure.`var`("clojure.core", "require").invoke(Symbol.intern("xtdb.node"))
    }

    private val START_NODE: IFn = Clojure.`var`("xtdb.node", "start-node")

    @JvmStatic
    fun startServer(port: Int = 9832): IXtdb {
        return START_NODE.invoke(PersistentHashMap.create(mapOf("xtdb/server".kw to mapOf("port".kw to port)))) as IXtdb
    }
}
