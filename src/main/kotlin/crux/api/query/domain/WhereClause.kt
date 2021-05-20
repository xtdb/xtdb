package crux.api.query.domain

import clojure.lang.Keyword
import clojure.lang.Symbol
import crux.api.query.domain.QuerySection.WhereSection
import crux.api.underware.pl
import crux.api.underware.prefix
import crux.api.underware.pv
import crux.api.underware.sym

sealed class WhereClause {
    data class HasKey(val symbol: Symbol, val key: Keyword): WhereClause() {
        override fun toEdn() = listOf(symbol, key).pv
    }

    data class HasKeyEqualTo(val document: Symbol, val key: Keyword, val value: Any): WhereClause() {
        override fun toEdn() = listOf(document, key, value).pv
    }

    data class Join(val type: Type, val body: WhereSection): WhereClause() {
        enum class Type(val symbol: Symbol) {
            NOT("not".sym),
            OR("or".sym)
        }

        override fun toEdn() = body.clauses.map(WhereClause::toEdn).prefix(type.symbol).pl
    }

    data class Predicate(val type: Type, val i: Symbol, val j: Any): WhereClause() {
        enum class Type(val symbol: Symbol) {
            EQ("==".sym),
            NEQ("!=".sym),
            GT(">".sym),
            GTE(">=".sym),
            LT("<".sym),
            LTE("<=".sym)
        }

        override fun toEdn() = listOf(
            listOf(type.symbol, i, j).pl
        ).pv
    }

    data class RuleInvocation(val name: Symbol, val parameters: List<Any>): WhereClause() {
        override fun toEdn() = parameters.prefix(name).pl
    }

    abstract fun toEdn(): Any
}
