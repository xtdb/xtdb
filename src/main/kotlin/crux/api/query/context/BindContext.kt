package crux.api.query.context

import clojure.lang.Symbol
import crux.api.query.domain.BindClause
import crux.api.query.domain.QuerySection.BindSection
import crux.api.underware.BuilderContext
import crux.api.underware.BuilderContextCompanion
import crux.api.underware.SimpleBuilderContext

class BindContext private constructor(): SimpleBuilderContext<BindClause, BindSection>(::BindSection) {
    companion object: BuilderContextCompanion<BindSection, BindContext>(::BindContext)

    operator fun BindClause.unaryPlus() = add(this)

    operator fun Symbol.unaryPlus() = +BindClause.SimpleBind(this)
    fun col(symbol: Symbol) = +BindClause.CollectionBind(symbol)
}