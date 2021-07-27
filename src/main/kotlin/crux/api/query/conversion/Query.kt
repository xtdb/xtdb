package crux.api.query.conversion

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import crux.api.CruxDocument
import crux.api.ICruxDatasource
import crux.api.query.context.Query1Context
import crux.api.query.context.Query2Context
import crux.api.query.context.Query3Context
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

fun <T : Any> ICruxDatasource.q(
    serde: CruxDocumentSerde<T>,
    vararg params: Any,
    block: Query1Context<T>.() -> Unit
): List<T> =
    query(Query1Context.build(block).toEdn(), *params)
        .map { result ->
            val map = result.single() as IPersistentMap
            map.let(CruxDocument::factory).run(serde::toObject)
        }

fun <T1: Any, T2: Any> ICruxDatasource.q(
    s1: CruxDocumentSerde<T1>,
    s2: CruxDocumentSerde<T2>,
    vararg params: Any,
    block: Query2Context<T1, T2>.() -> Unit
): List<Pair<T1, T2>> =
    query(Query2Context.build(block).toEdn(), *params)
        .map { result ->
            val d1 = result[0] as IPersistentMap
            val d2 = result[1] as IPersistentMap
            d1.let(CruxDocument::factory).run(s1::toObject) to d2.let(CruxDocument::factory).run(s2::toObject)
        }

fun <T1: Any, T2: Any, T3: Any> ICruxDatasource.q(
    s1: CruxDocumentSerde<T1>,
    s2: CruxDocumentSerde<T2>,
    s3: CruxDocumentSerde<T3>,
    vararg params: Any,
    block: Query3Context<T1, T2, T3>.() -> Unit
): List<Triple<T1, T2, T3>> =
    query(Query3Context.build(block).toEdn(), *params)
        .map { result ->
            val d1 = result[0] as IPersistentMap
            val d2 = result[1] as IPersistentMap
            val d3 = result[2] as IPersistentMap
            Triple(
                d1.let(CruxDocument::factory).run(s1::toObject),
                d2.let(CruxDocument::factory).run(s2::toObject),
                d3.let(CruxDocument::factory).run(s3::toObject)
            )
        }