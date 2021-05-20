package crux.api.query.domain

import clojure.lang.Symbol
import crux.api.query.context.QueryContext
import crux.api.underware.pl
import crux.api.underware.pv
import crux.api.underware.sym

data class FindSection(val clauses: List<FindClause>): QuerySection {
    override val key = QueryContext.FIND
    override fun toEdn() = clauses.map(FindClause::toEdn).pv
}

sealed class FindClause {
    data class SimpleFind(val symbol: Symbol): FindClause() {
        override fun toEdn() = symbol
    }

    data class Aggregate(val type: AggregateType, val symbol: Symbol): FindClause() {
        override fun toEdn() = listOf(type.symbol, symbol).pl
    }

    data class AggregateWithNumber(val type: AggregateType, val n: Int, val symbol: Symbol): FindClause() {
        override fun toEdn() = listOf(type, n, symbol).pl
    }

    abstract fun toEdn(): Any
}

enum class AggregateType(val symbol: Symbol) {
    SUM("sum".sym),
    MIN("min".sym),
    MAX("max".sym),
    COUNT("count".sym),
    AVG("avg".sym),
    MEDIAN("median".sym),
    VARIANCE("variance".sym),
    STDDEV("stddev".sym),
    RAND("rand".sym),
    SAMPLE("sample".sym),
    DISTINCT("distinct".sym)
}