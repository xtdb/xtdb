package crux.api.query.conversion

import clojure.lang.Keyword
import crux.api.ICruxDatasource
import crux.api.query.context.QueryContext
import crux.api.query.conversion.QuerySectionKey.*
import crux.api.query.domain.*
import crux.api.query.domain.QuerySection.*
import crux.api.underware.kw
import crux.api.underware.pam
import crux.api.underware.pv

enum class QuerySectionKey(val keyword: Keyword) {
    FIND("find".kw),
    WHERE("where".kw),
    IN("in".kw),
    ORDER("order-by".kw),
    OFFSET("offset".kw),
    LIMIT("limit".kw),
    RULES("rules".kw)
}

fun Query.toEdn() =
    sections
        .map(QuerySection::toEdn)
        .toMap()
        .mapKeys { it.key.keyword }
        .pam

fun QuerySection.toEdn() = when (this) {
    is BindSection -> IN to clauses.map(BindClause::toEdn).pv
    is OrderSection -> ORDER to clauses.map(OrderClause::toEdn).pv
    is FindSection -> FIND to clauses.map(FindClause::toEdn).pv
    is LimitSection -> LIMIT to limit
    is OffsetSection -> OFFSET to offset
    is WhereSection -> WHERE to clauses.map(WhereClause::toEdn).pv
    is RulesSection -> RULES to rules.map(RuleDefinition::toEdn).pv
}

fun ICruxDatasource.q(vararg params: Any, block: QueryContext.() -> Unit): MutableCollection<MutableList<*>> =
    query(QueryContext.build(block).toEdn(), *params)