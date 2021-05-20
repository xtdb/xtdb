package crux.api.query.domain

import crux.api.underware.pv

sealed class QuerySection {
    data class BindSection(val clauses: List<BindClause>): QuerySection() {
        fun toEdn() = clauses.map(BindClause::toEdn).pv
    }

    data class FindSection(val clauses: List<FindClause>): QuerySection() {
        fun toEdn() = clauses.map(FindClause::toEdn).pv
    }

    data class LimitSection(val limit: Int): QuerySection() {
        fun toEdn() = limit
    }

    data class OffsetSection(val offset: Int): QuerySection() {
        fun toEdn() = offset
    }

    data class OrderSection(val clauses: List<OrderClause>): QuerySection() {
        fun toEdn() = clauses.map(OrderClause::toEdn).pv
    }

    data class RulesSection(val rules: List<RuleDefinition>): QuerySection() {
        fun toEdn() = rules.map(RuleDefinition::toEdn).pv
    }

    data class WhereSection(val clauses: List<WhereClause>): QuerySection() {
        fun toEdn() = clauses.map(WhereClause::toEdn).pv
    }
}
