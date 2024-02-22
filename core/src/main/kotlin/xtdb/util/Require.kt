@file:JvmSynthetic
package xtdb.util

import clojure.java.api.Clojure
import clojure.lang.IFn
import clojure.lang.Symbol
import clojure.lang.Var

private val REQUIRING_RESOLVE = Clojure.`var`("clojure.core", "requiring-resolve")

fun requiringResolve(nsname: String) = REQUIRING_RESOLVE.invoke(Symbol.intern(nsname)) as Var
