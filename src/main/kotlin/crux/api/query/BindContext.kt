package crux.api.query

import clojure.lang.Symbol
import crux.api.clj
import crux.api.pv

class BindContext private constructor() {
    companion object {
        fun build(block: BindContext.() -> Unit) = BindContext().also(block).build()

        private val COL = "...".clj
    }

    private val bindings = mutableListOf<Any>()

    operator fun Symbol.unaryPlus() {
        bindings.add(this)
    }

    fun col(symbol: Symbol) {
        bindings.add(listOf(symbol, COL).pv)
    }

    private fun build() = bindings.pv
}