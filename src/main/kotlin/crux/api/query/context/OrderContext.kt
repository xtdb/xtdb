package crux.api.query.context

import clojure.lang.Symbol
import crux.api.query.domain.OrderClause
import crux.api.query.domain.OrderClause.Direction
import crux.api.query.domain.OrderClause.Direction.*
import crux.api.query.domain.QuerySection.OrderSection
import crux.api.underware.BuilderContext

class OrderContext private constructor(): BuilderContext<OrderSection> {
    companion object {
        fun build(block: OrderContext.() -> Unit) = OrderContext().also(block).build()
    }

    private val clauses = mutableListOf<OrderClause>()

    private fun add(clause: OrderClause) {
        clauses.add(clause)
    }

    private fun add(symbol: Symbol, direction: Direction) = +OrderClause(symbol, direction)

    operator fun OrderClause.unaryPlus() = add(this)
    operator fun Symbol.unaryPlus() = add(this, ASC)
    operator fun Symbol.unaryMinus() = add(this, DESC)

    override fun build() = OrderSection(clauses)
}