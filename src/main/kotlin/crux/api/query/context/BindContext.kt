package crux.api.query.context

import clojure.lang.Symbol
import crux.api.query.domain.BindClause
import crux.api.query.domain.QuerySection.BindSection
import crux.api.underware.BuilderContext
import crux.api.underware.BuilderContextCompanion

class BindContext private constructor(): BuilderContext<BindSection> {
    companion object: BuilderContextCompanion<BindSection, BindContext>(::BindContext)

    private val clauses = mutableListOf<BindClause>()

    private fun add(clause: BindClause) {
        clauses.add(clause)
    }

    operator fun BindClause.unaryPlus() = add(this)

    operator fun Symbol.unaryPlus() = +BindClause.SimpleBind(this)

    fun col(symbol: Symbol) = +BindClause.CollectionBind(symbol)

    override fun build() = BindSection(clauses)
}