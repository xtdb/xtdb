package xtdb

import clojure.lang.Keyword
import clojure.lang.Symbol

val String.kw: Keyword get() = Keyword.intern(this)

val String.symbol: Symbol get() = Symbol.intern(this)

