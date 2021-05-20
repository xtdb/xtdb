package crux.api.query.domain

import clojure.lang.Keyword
import clojure.lang.Symbol
import crux.api.query.context.QueryContext
import crux.api.underware.pl
import crux.api.underware.prefix
import crux.api.underware.pv
import crux.api.underware.sym

data class WhereSection(val clauses: List<WhereClause>): QuerySection {
    override val key = QueryContext.WHERE
    override fun toEdn() = clauses.map(WhereClause::toEdn).pv
}

sealed class WhereClause {

    data class HasKey(val symbol: Symbol, val key: Keyword): WhereClause() {
        override fun toEdn() = listOf(symbol, key).pv
    }

    data class HasKeyEqualTo(val document: Symbol, val key: Keyword, val value: Any): WhereClause() {
        override fun toEdn() = listOf(document, key, value).pv
    }

    data class Join(val joinType: JoinType, val body: WhereSection): WhereClause() {
        override fun toEdn() = body.clauses.map(WhereClause::toEdn).prefix(joinType.symbol).pl
    }

    data class Predicate(val predicateType: PredicateType, val i: Symbol, val j: Any): WhereClause() {
        override fun toEdn() = listOf(
            listOf(predicateType.symbol, i, j).pl
        ).pv
    }

    data class RuleInvocation(val name: Symbol, val parameters: List<Any>): WhereClause() {
        override fun toEdn() = parameters.prefix(name).pl
    }

    abstract fun toEdn(): Any
}

enum class JoinType(val symbol: Symbol) {
    NOT("not".sym),
    OR("or".sym)
}

enum class PredicateType(val symbol: Symbol) {
    EQ("==".sym),
    NEQ("!=".sym),
    GT(">".sym),
    GTE(">=".sym),
    LT("<".sym),
    LTE("<=".sym)
}



