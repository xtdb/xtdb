package xtdb.api.query.conversion

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import xtdb.api.IXtdbDatasource
import xtdb.api.XtdbDocument
import xtdb.api.query.context.Query1Context
import xtdb.api.query.context.Query2Context
import xtdb.api.query.context.Query3Context
import xtdb.api.query.context.QueryContext
import xtdb.api.query.conversion.QuerySectionKey.*
import xtdb.api.query.domain.*
import xtdb.api.query.domain.QuerySection.*
import xtdb.api.underware.kw
import xtdb.api.underware.pam
import xtdb.api.underware.pv

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

fun IXtdbDatasource.q(vararg params: Any, block: QueryContext.() -> Unit): MutableCollection<MutableList<*>> =
    query(QueryContext.build(block).toEdn(), *params)

fun <T : Any> IXtdbDatasource.q(
    serde: XtdbDocumentSerde<T>,
    vararg params: Any,
    block: Query1Context<T>.() -> Unit
): List<T> =
    query(Query1Context.build(block).toEdn(), *params)
        .map { result ->
            val map = result.single() as IPersistentMap
            map.let(XtdbDocument::factory).run(serde::toObject)
        }

fun <T1: Any, T2: Any> IXtdbDatasource.q(
    s1: XtdbDocumentSerde<T1>,
    s2: XtdbDocumentSerde<T2>,
    vararg params: Any,
    block: Query2Context<T1, T2>.() -> Unit
): List<Pair<T1, T2>> =
    query(Query2Context.build(block).toEdn(), *params)
        .map { result ->
            val d1 = result[0] as IPersistentMap
            val d2 = result[1] as IPersistentMap
            d1.let(XtdbDocument::factory).run(s1::toObject) to d2.let(XtdbDocument::factory).run(s2::toObject)
        }

fun <T1: Any, T2: Any, T3: Any> IXtdbDatasource.q(
    s1: XtdbDocumentSerde<T1>,
    s2: XtdbDocumentSerde<T2>,
    s3: XtdbDocumentSerde<T3>,
    vararg params: Any,
    block: Query3Context<T1, T2, T3>.() -> Unit
): List<Triple<T1, T2, T3>> =
    query(Query3Context.build(block).toEdn(), *params)
        .map { result ->
            val d1 = result[0] as IPersistentMap
            val d2 = result[1] as IPersistentMap
            val d3 = result[2] as IPersistentMap
            Triple(
                d1.let(XtdbDocument::factory).run(s1::toObject),
                d2.let(XtdbDocument::factory).run(s2::toObject),
                d3.let(XtdbDocument::factory).run(s3::toObject)
            )
        }