package crux.api.query.bind

import clojure.lang.Symbol
import crux.api.query.QueryContext
import crux.api.query.QuerySection
import crux.api.underware.clj
import crux.api.underware.pv

data class BindSection(val clauses: List<BindClause>): QuerySection {
    override val key = QueryContext.IN

    override fun toEdn() = clauses.map(BindClause::toEdn).pv
}

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