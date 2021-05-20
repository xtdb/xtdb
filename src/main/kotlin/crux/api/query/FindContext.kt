package crux.api.query

import clojure.lang.Symbol
import crux.api.pl
import crux.api.pv
import crux.api.sym

class FindContext private constructor() {
    companion object {
        fun build(block: FindContext.() -> Unit) = FindContext().also(block).build()

        private val SUM = "sum".sym
        private val MIN = "min".sym
        private val MAX = "max".sym
        private val COUNT = "count".sym
        private val AVG = "avg".sym
        private val MEDIAN = "median".sym
        private val VARIANCE = "variance".sym
        private val STDDEV = "stddev".sym
        private val RAND = "rand".sym
        private val SAMPLE = "sample".sym
        private val DISTINCT = "distinct".sym
    }

    private val args = mutableListOf<Any>()

    operator fun Symbol.unaryPlus() {
        args.add(this)
    }

    operator fun String.unaryPlus() {
        args.add(sym)
    }

    private fun aggregate(type: Symbol, symbol: Symbol) {
        args.add(listOf(type, symbol).pl)
    }

    private fun aggregate(type: Symbol, n: Int, symbol: Symbol) {
        args.add(listOf(type, n, symbol).pl)
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

    private fun build() = args.pv
}