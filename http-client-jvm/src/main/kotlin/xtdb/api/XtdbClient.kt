package xtdb.api

import clojure.java.api.Clojure
import clojure.lang.IFn
import clojure.lang.Symbol
import java.net.URL

object XtdbClient {
    init {
        Clojure.`var`("clojure.core", "require").invoke(Symbol.intern("xtdb.client"))
    }

    private val OPEN_CLIENT: IFn = Clojure.`var`("xtdb.client", "start-client")

    @JvmStatic
    fun openClient(url: URL): IXtdb {
        return OPEN_CLIENT.invoke(url.toString()) as IXtdb
    }
}
