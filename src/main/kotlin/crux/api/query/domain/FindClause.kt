package crux.api.query.domain

import clojure.lang.Symbol
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

    data class SimpleFind(val symbol: Symbol): FindClause()

    data class Aggregate(val type: AggregateType, val symbol: Symbol): FindClause()

    data class AggregateWithNumber(val type: AggregateType, val n: Int, val symbol: Symbol): FindClause()

    data class Pull(val symbol: Symbol, val spec: ProjectionSpec): FindClause()
}

