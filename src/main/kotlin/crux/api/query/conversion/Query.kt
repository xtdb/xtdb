package crux.api.query.conversion

import clojure.lang.Keyword
import crux.api.ICruxDatasource
import crux.api.query.context.QueryContext
import crux.api.query.conversion.QuerySectionKey.*
import crux.api.query.domain.*
import crux.api.query.domain.QuerySection.*
import crux.api.underware.kw
import crux.api.underware.pam

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
        .map {
            when (it) {
                is BindSection -> IN to it.toEdn()
                is OrderSection -> ORDER to it.toEdn()
                is FindSection -> FIND to it.toEdn()
                is LimitSection -> LIMIT to it.toEdn()
                is OffsetSection -> OFFSET to it.toEdn()
                is WhereSection -> WHERE to it.toEdn()
                is RulesSection -> RULES to it.toEdn()
            }
        }
        .toMap()
        .mapKeys { it.key.keyword }
        .pam

fun ICruxDatasource.q(vararg params: Any, block: QueryContext.() -> Unit): MutableCollection<MutableList<*>> =
    query(QueryContext.build(block).toEdn(), *params)