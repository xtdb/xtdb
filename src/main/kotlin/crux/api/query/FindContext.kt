package crux.api.query

import clojure.lang.Symbol
import crux.api.pv
import crux.api.sym

class FindContext private constructor() {
    companion object {
        fun build(block: FindContext.() -> Unit) = FindContext().also(block).build()
    }
    private val args = mutableListOf<Any>()

    operator fun Symbol.unaryPlus() {
        args.add(this)
    }

    operator fun String.unaryPlus() {
        args.add(sym)
    }

    private fun build() = args.pv
}