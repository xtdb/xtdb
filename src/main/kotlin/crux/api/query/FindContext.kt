package crux.api.query

import clojure.lang.Symbol
import crux.api.pv
import crux.api.sym

class FindContext {
    private val args = mutableListOf<Any>()

    operator fun Symbol.unaryPlus() {
        args.add(this)
    }

    operator fun String.unaryPlus() {
        args.add(sym)
    }

    fun build() = args.pv
}