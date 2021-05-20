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

    private val data = mutableListOf<OrderClause>()

    private fun add(symbol: Symbol, direction: Direction) {
        data.add(OrderClause(symbol, direction))
    }

    operator fun Symbol.unaryPlus() = add(this, ASC)
    operator fun Symbol.unaryMinus() = add(this, DESC)

    override fun build() = OrderSection(data)
}