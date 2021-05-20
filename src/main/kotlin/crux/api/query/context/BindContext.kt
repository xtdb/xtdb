package crux.api.query.context

import clojure.lang.Symbol
import crux.api.query.domain.BindClause
import crux.api.query.domain.QuerySection.BindSection
import crux.api.underware.BuilderContext

class BindContext private constructor(): BuilderContext<BindSection> {
    companion object {
        fun build(block: BindContext.() -> Unit) = BindContext().also(block).build()
    }

    private val clauses = mutableListOf<BindClause>()

    private fun add(clause: BindClause) {
        clauses.add(clause)
    }

    operator fun BindClause.unaryPlus() = add(this)

    operator fun Symbol.unaryPlus() = +BindClause.SimpleBind(this)

    fun col(symbol: Symbol) = +BindClause.CollectionBind(symbol)

    override fun build() = BindSection(clauses)
}