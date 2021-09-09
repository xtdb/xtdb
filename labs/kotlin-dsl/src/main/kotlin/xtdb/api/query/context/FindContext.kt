package xtdb.api.query.context

import clojure.lang.Symbol
import xtdb.api.query.domain.FindClause
import xtdb.api.query.domain.FindClause.*
import xtdb.api.query.domain.FindClause.AggregateType.*
import xtdb.api.query.domain.PullSpec
import xtdb.api.query.domain.PullSpec.Item.ALL
import xtdb.api.query.domain.QuerySection.FindSection
import xtdb.api.underware.BuilderContext
import xtdb.api.underware.SimpleBuilderContext

class FindContext private constructor(): SimpleBuilderContext<FindClause, FindSection>(::FindSection) {
    companion object: BuilderContext.Companion<FindSection, FindContext>(::FindContext)

    operator fun FindClause.unaryPlus() = add(this)

    operator fun Symbol.unaryPlus() = +SimpleFind(this)

    private fun aggregate(type: AggregateType, symbol: Symbol) = +Aggregate(type, symbol)

    private fun aggregate(type: AggregateType, n: Int, symbol: Symbol) = +AggregateWithNumber(type, n, symbol)

    fun sum(symbol: Symbol) = aggregate(SUM, symbol)
    fun min(symbol: Symbol) = aggregate(MIN, symbol)
    fun max(symbol: Symbol) = aggregate(MAX, symbol)
    fun count(symbol: Symbol) = aggregate(COUNT, symbol)
    fun avg(symbol: Symbol) = aggregate(AVG, symbol)
    fun median(symbol: Symbol) = aggregate(MEDIAN, symbol)
    fun variance(symbol: Symbol) = aggregate(VARIANCE, symbol)
    fun stdDev(symbol: Symbol) = aggregate(STDDEV, symbol)
    fun rand(n: Int, symbol: Symbol) = aggregate(RAND, n, symbol)
    fun sample(n: Int, symbol: Symbol) = aggregate(SAMPLE, n, symbol)
    fun distinct(symbol: Symbol) = aggregate(DISTINCT, symbol)

    fun pull(symbol: Symbol, block: PullSpecContext.() -> Unit) = +Pull(symbol, PullSpecContext.build(block))
    fun pullAll(symbol: Symbol) = +Pull(symbol, PullSpec(listOf(ALL)))
}