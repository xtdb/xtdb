package xtdb.api.query.context

import clojure.lang.Symbol
import xtdb.api.query.domain.OrderClause
import xtdb.api.query.domain.OrderClause.Direction
import xtdb.api.query.domain.OrderClause.Direction.ASC
import xtdb.api.query.domain.OrderClause.Direction.DESC
import xtdb.api.query.domain.QuerySection.OrderSection
import xtdb.api.underware.BuilderContext
import xtdb.api.underware.SimpleBuilderContext

class OrderContext private constructor(): SimpleBuilderContext<OrderClause, OrderSection>(::OrderSection) {
    companion object: BuilderContext.Companion<OrderSection, OrderContext>(::OrderContext)

    private fun add(symbol: Symbol, direction: Direction) = +OrderClause(symbol, direction)

    operator fun OrderClause.unaryPlus() = add(this)
    operator fun Symbol.unaryPlus() = add(this, ASC)
    operator fun Symbol.unaryMinus() = add(this, DESC)
}