package crux.api.query.context

import clojure.lang.Symbol
import crux.api.query.domain.QuerySection.RulesSection
import crux.api.query.domain.RuleDefinition
import crux.api.underware.BuilderContext
import crux.api.underware.SimpleBuilderContext

class RulesContext private constructor(): SimpleBuilderContext<RuleDefinition, RulesSection>(::RulesSection) {
    companion object: BuilderContext.Companion<RulesSection, RulesContext>(::RulesContext)

    operator fun RuleDefinition.unaryPlus() = add(this)

    data class RuleDeclaration(val name: Symbol)

    fun def(name: Symbol) = RuleDeclaration(name)

    operator fun RuleDeclaration.invoke(vararg params: Symbol, block: WhereContext.() -> Unit) =
        +RuleDefinition(
            name,
            params.toList(),
            WhereContext.build(block)
        )
}