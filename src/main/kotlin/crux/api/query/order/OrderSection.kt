package crux.api.query.order

import clojure.lang.Keyword
import clojure.lang.Symbol
import crux.api.query.QueryContext
import crux.api.query.QuerySection
import crux.api.underware.kw
import crux.api.underware.pv

data class OrderSection(val clauses: List<OrderClause>): QuerySection {
    override val key = QueryContext.ORDER

    override fun toEdn() = clauses.map(OrderClause::toEdn).pv
}

data class OrderClause(val symbol: Symbol, val direction: OrderDirection) {
    fun toEdn() = listOf(symbol, direction.keyword).pv
}

enum class OrderDirection(val keyword: Keyword) {
    ASC("asc".kw),
    DESC("desc".kw)
}