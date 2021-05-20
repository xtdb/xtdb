package crux.api.query.rules

import clojure.lang.Symbol
import crux.api.query.where.WhereContext
import crux.api.underware.BuilderContext

class RulesContext private constructor(): BuilderContext<RulesSection> {
    companion object {
        fun build(block: RulesContext.() -> Unit) = RulesContext().also(block).build()
    }

    private val rules = mutableListOf<RuleDefinition>()

    data class RuleDeclaration(val name: Symbol)

    fun def(name: Symbol) = RuleDeclaration(name)

    operator fun RuleDeclaration.invoke(vararg params: Symbol, block: WhereContext.() -> Unit) {
        rules.add(
            RuleDefinition(
                name,
                params.toList(),
                WhereContext.build(block)
            )
        )
    }

    override fun build() = RulesSection(rules)
}