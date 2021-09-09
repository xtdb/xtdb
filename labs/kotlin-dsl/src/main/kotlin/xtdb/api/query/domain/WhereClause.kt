package xtdb.api.query.domain

import clojure.lang.Keyword
import clojure.lang.Symbol
import xtdb.api.query.domain.QuerySection.WhereSection
import xtdb.api.underware.sym

sealed class WhereClause {
    data class HasKey(val symbol: Symbol, val key: Keyword): WhereClause()

    data class HasKeyEqualTo(val document: Symbol, val key: Keyword, val value: Any): WhereClause()

    data class Join(val type: Type, val body: WhereSection): WhereClause() {
        enum class Type(val symbol: Symbol) {
            NOT("not".sym),
            OR("or".sym)
        }
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
    }

    data class RuleInvocation(val name: Symbol, val parameters: List<Any>): WhereClause()

    data class SetToFunction(val target: Symbol, val type: Type, val i: Symbol, val j: Any): WhereClause() {
        enum class Type(val symbol: Symbol) {
            PLUS("+".sym),
            MINUS("-".sym),
            TIMES("*".sym),
            DIVIDE("/".sym)
        }
    }
}
