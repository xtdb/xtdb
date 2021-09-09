package xtdb.api.query.context

import clojure.lang.Symbol
import xtdb.api.query.domain.BindClause
import xtdb.api.query.domain.QuerySection.BindSection
import xtdb.api.underware.BuilderContext
import xtdb.api.underware.SimpleBuilderContext

class BindContext private constructor(): SimpleBuilderContext<BindClause, BindSection>(::BindSection) {
    companion object: BuilderContext.Companion<BindSection, BindContext>(::BindContext)

    operator fun BindClause.unaryPlus() = add(this)

    operator fun Symbol.unaryPlus() = +BindClause.SimpleBind(this)
    fun col(symbol: Symbol) = +BindClause.CollectionBind(symbol)
}