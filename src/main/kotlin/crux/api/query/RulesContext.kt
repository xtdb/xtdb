package crux.api.query

import clojure.lang.PersistentVector
import clojure.lang.Symbol
import crux.api.pl
import crux.api.prefix
import crux.api.pv

class RulesContext private constructor() {
    companion object {
        fun build(block: RulesContext.() -> Unit) = RulesContext().also(block).build()
    }

    private val rules = mutableListOf<PersistentVector>()

    data class RuleDeclaration(val name: Symbol)

    fun def(name: Symbol) = RuleDeclaration(name)

    operator fun RuleDeclaration.invoke(vararg params: Symbol, block: WhereContext.() -> Unit) {
        rules.add(
            WhereContext.clauses(block)
                .prefix(
                    params.toList().prefix(name).pl
                ).pv
        )
    }

    private fun build() = rules.pv
}