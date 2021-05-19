package crux.api.query

import clojure.lang.Keyword
import clojure.lang.PersistentVector
import clojure.lang.Symbol
import crux.api.kw
import crux.api.pv

class OrderContext private constructor() {
    companion object {
        fun build(block: OrderContext.() -> Unit) = OrderContext().also(block).build()

        private val ASC = "asc".kw
        private val DESC = "desc".kw
    }

    private val data = mutableListOf<PersistentVector>()

    private fun add(symbol: Symbol, direction: Keyword) {
        data.add(listOf(symbol, direction).pv)
    }

    operator fun Symbol.unaryPlus() = add(this, ASC)
    operator fun Symbol.unaryMinus() = add(this, DESC)

    private fun build() = data.pv
}