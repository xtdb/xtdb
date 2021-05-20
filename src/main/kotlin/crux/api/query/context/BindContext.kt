package crux.api.query.context

import clojure.lang.Symbol
import crux.api.query.domain.BindClause
import crux.api.query.domain.QuerySection.BindSection
import crux.api.underware.BuilderContext

class BindContext private constructor(): BuilderContext<BindSection> {
    companion object {
        fun build(block: BindContext.() -> Unit) = BindContext().also(block).build()
    }

    private val bindings = mutableListOf<BindClause>()

    operator fun Symbol.unaryPlus() {
        bindings.add(BindClause.SimpleBind(this))
    }

    fun col(symbol: Symbol) {
        bindings.add(BindClause.CollectionBind(symbol))
    }

    override fun build() = BindSection(bindings)
}