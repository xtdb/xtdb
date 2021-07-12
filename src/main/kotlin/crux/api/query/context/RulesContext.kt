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
    data class BoundRuleDeclaration(val name: Symbol, val boundParameters: List<Symbol>)

    fun def(name: Symbol) = RuleDeclaration(name)

    operator fun RuleDeclaration.invoke(vararg params: Symbol, block: WhereContext.() -> Unit) =
        +RuleDefinition(
            name,
            emptyList(),
            params.toList(),
            WhereContext.build(block)
        )

    operator fun RuleDeclaration.get(vararg boundParameters: Symbol) =
        BoundRuleDeclaration(name, boundParameters.toList())

    operator fun BoundRuleDeclaration.invoke(vararg params: Symbol, block: WhereContext.() -> Unit) =
        +RuleDefinition(
            name,
            boundParameters,
            params.toList(),
            WhereContext.build(block)
        )
}