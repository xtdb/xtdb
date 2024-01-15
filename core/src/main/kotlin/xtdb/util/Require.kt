package xtdb.util

import clojure.java.api.Clojure
import clojure.lang.IFn
import clojure.lang.Symbol

private val REQUIRE = Clojure.`var`("clojure.core", "require")

internal fun requiringResolve(ns: String, name: String): IFn {
    REQUIRE.invoke(Symbol.intern(ns))
    return Clojure.`var`(ns, name)
}

