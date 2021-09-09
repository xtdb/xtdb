package xtdb.api.query.context

import clojure.lang.Symbol
import xtdb.api.query.domain.QuerySection.RulesSection
import xtdb.api.query.domain.RuleDefinition
import xtdb.api.underware.BuilderContext
import xtdb.api.underware.SimpleBuilderContext

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