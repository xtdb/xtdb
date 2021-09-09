package xtdb.api.query.domain


sealed class QuerySection {
    data class BindSection(val clauses: List<BindClause>): QuerySection()

    data class FindSection(val clauses: List<FindClause>): QuerySection()

    data class LimitSection(val limit: Int): QuerySection()

    data class OffsetSection(val offset: Int): QuerySection()

    data class OrderSection(val clauses: List<OrderClause>): QuerySection()

    data class RulesSection(val rules: List<RuleDefinition>): QuerySection()

    data class WhereSection(val clauses: List<WhereClause>): QuerySection()
}
