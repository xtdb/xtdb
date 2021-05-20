package crux.api.query.domain

import clojure.lang.Symbol
import crux.api.underware.clj
import crux.api.underware.pv

sealed class BindClause {
    data class SimpleBind(val symbol: Symbol): BindClause() {
        override fun toEdn() = symbol
    }

    data class CollectionBind(val symbol: Symbol): BindClause() {
        companion object {
            private val COL = "...".clj
        }
        override fun toEdn() = listOf(symbol, COL).pv
    }

    abstract fun toEdn(): Any
}