package crux.api.query.domain

import clojure.lang.Symbol
import crux.api.query.context.QueryContext
import crux.api.underware.pl
import crux.api.underware.prefix
import crux.api.underware.pv

data class RulesSection(val rules: List<RuleDefinition>): QuerySection {
    override val key = QueryContext.RULES
    override fun toEdn() = rules.map(RuleDefinition::toEdn).pv
}

data class RuleDefinition(val name: Symbol, val parameters: List<Symbol>, val body: WhereSection) {
    fun toEdn() = body.clauses.map(WhereClause::toEdn).prefix(
        parameters.prefix(name).pl
    ).pv
}