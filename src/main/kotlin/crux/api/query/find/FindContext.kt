package crux.api.query.find

import clojure.lang.Symbol
import crux.api.query.find.AggregateType.*
import crux.api.underware.BuilderContext

class FindContext private constructor(): BuilderContext<FindSection> {
    companion object {
        fun build(block: FindContext.() -> Unit) = FindContext().also(block).build()
    }

    private val args = mutableListOf<FindClause>()

    operator fun Symbol.unaryPlus() {
        args.add(FindClause.SimpleFind(this))
    }

    private fun aggregate(type: AggregateType, symbol: Symbol) {
        args.add(FindClause.Aggregate(type, symbol))
    }

    private fun aggregate(type: AggregateType, n: Int, symbol: Symbol) {
        args.add(FindClause.AggregateWithNumber(type, n, symbol))
    }

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

    override fun build() = FindSection(args)
}