package crux.api.query.context

import clojure.lang.Symbol
import crux.api.query.domain.FindClause
import crux.api.query.domain.FindClause.*
import crux.api.query.domain.FindClause.AggregateType.*
import crux.api.query.domain.ProjectionSpec
import crux.api.query.domain.ProjectionSpec.Item.all
import crux.api.query.domain.QuerySection.FindSection
import crux.api.underware.BuilderContext
import crux.api.underware.SimpleBuilderContext

class FindContext private constructor(): SimpleBuilderContext<FindClause, FindSection>(::FindSection) {
    companion object: BuilderContext.Companion<FindSection, FindContext>(::FindContext)

    operator fun FindClause.unaryPlus() = add(this)

    operator fun Symbol.unaryPlus() = +SimpleFind(this)

    private fun aggregate(type: AggregateType, symbol: Symbol) =
        +Aggregate(type, symbol)

    private fun aggregate(type: AggregateType, n: Int, symbol: Symbol) =
        +AggregateWithNumber(type, n, symbol)

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

    fun pull(symbol: Symbol, block: ProjectionSpecContext.() -> Unit) = +Pull(symbol, ProjectionSpecContext.build(block))
    fun pullAll(symbol: Symbol) = +Pull(symbol, ProjectionSpec(listOf(all)))
}