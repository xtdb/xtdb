package crux.api.query

import clojure.lang.Keyword
import clojure.lang.PersistentVector
import clojure.lang.Symbol
import crux.api.pl
import crux.api.pv
import crux.api.sym

class WhereContext {
    companion object {
        fun build(block: WhereContext.() -> Unit) = WhereContext().also(block).build()

        private val NOT = "not".sym
        private val OR = "or".sym
        private val EQ = "==".sym
        private val NEQ = "!=".sym
        private val GT = ">".sym
        private val GTE = ">=".sym
        private val LT = "<".sym
        private val LTE = "<=".sym
    }

    private val clauses = mutableListOf<Any>()

    private var hangingClause: Any? = null

    data class SymbolAndKey(val symbol: Symbol, val key: Keyword)

    infix fun Symbol.has(key: Keyword) =
        SymbolAndKey(this, key).also {
            lockIn()
            hangingClause = listOf(this, key).pv
        }

    infix fun SymbolAndKey.eq(value: Any) {
        hangingClause = listOf(symbol, key, value).pv
    }

    private fun join(type: Symbol, block: WhereContext.() -> Unit) {
        lockIn()
        hangingClause =
            (listOf(type) +
                    WhereContext()
                        .also(block)
                        .apply(WhereContext::lockIn)
                        .clauses).pl
    }

    fun not(block: WhereContext.() -> Unit) = join(NOT, block)
    fun or(block: WhereContext.() -> Unit) = join(OR, block)

    private fun pred(symbol: Symbol, i: Symbol, j: Any) {
        lockIn()
        hangingClause = listOf(
            listOf(symbol, i, j).pl
        ).pv
    }

    infix fun Symbol.gt(other: Any) = pred(GT, this, other)
    infix fun Symbol.lt(other: Any) = pred(LT, this, other)
    infix fun Symbol.gte(other: Any) = pred(GTE, this, other)
    infix fun Symbol.lte(other: Any) = pred(LTE, this, other)
    infix fun Symbol.eq(other: Any) = pred(EQ, this, other)
    infix fun Symbol.neq(other: Any) = pred(NEQ, this, other)

    private fun lockIn() {
        hangingClause?.run(clauses::add)
        hangingClause = null
    }

    fun build(): PersistentVector {
        lockIn()
        return clauses.pv
    }
}