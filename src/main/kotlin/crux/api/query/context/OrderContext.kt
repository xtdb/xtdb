package crux.api.query.context

import clojure.lang.Symbol
import crux.api.query.domain.OrderClause
import crux.api.query.domain.OrderClause.Direction
import crux.api.query.domain.OrderClause.Direction.*
import crux.api.query.domain.QuerySection.OrderSection
import crux.api.underware.BuilderContext
import crux.api.underware.BuilderContextCompanion
import crux.api.underware.SimpleBuilderContext

class OrderContext private constructor(): SimpleBuilderContext<OrderClause, OrderSection>(::OrderSection) {
    companion object: BuilderContextCompanion<OrderSection, OrderContext>(::OrderContext)

    private fun add(symbol: Symbol, direction: Direction) = +OrderClause(symbol, direction)

    operator fun OrderClause.unaryPlus() = add(this)
    operator fun Symbol.unaryPlus() = add(this, ASC)
    operator fun Symbol.unaryMinus() = add(this, DESC)
}