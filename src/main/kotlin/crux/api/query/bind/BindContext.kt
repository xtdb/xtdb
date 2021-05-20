package crux.api.query.bind

import clojure.lang.Symbol
import crux.api.underware.BuilderContext
import crux.api.underware.clj
import crux.api.underware.pv

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