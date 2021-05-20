package crux.api.query.domain

import clojure.lang.Symbol
import crux.api.underware.pl
import crux.api.underware.sym

sealed class FindClause {
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

