package xtdb.api.query

import kotlinx.serialization.Serializable
import xtdb.api.query.Expr.Param
import xtdb.api.query.Exprs.lVar
import xtdb.api.query.XtqlQuery.OrderDirection.ASC
import xtdb.api.query.XtqlQuery.OrderDirection.DESC
import xtdb.api.query.XtqlQuery.OrderNulls.FIRST
import xtdb.api.query.XtqlQuery.OrderNulls.LAST

sealed interface Query

@Serializable
data class SqlQuery(@JvmField val sql: String) : Query

sealed interface XtqlQuery : Query {

    sealed interface QueryTail

    sealed interface UnifyClause

    data class Pipeline(@JvmField val query: XtqlQuery, @JvmField val tails: List<QueryTail>) : XtqlQuery

    data class Unify(@JvmField val clauses: List<UnifyClause>) : XtqlQuery

    data class From(
        @JvmField val table: String,
        @JvmField val bindings: List<Binding>? = null,
        @JvmField val forValidTime: TemporalFilter? = null,
        @JvmField val forSystemTime: TemporalFilter? = null,
        @JvmField val projectAllCols: Boolean = false,
    ) : XtqlQuery, UnifyClause {

        constructor(table: String, bindings: List<Binding>) : this(table, bindings, null, null, false)

        class Builder(private val table: String) : Binding.ABuilder<Builder, From>() {
            private var forValidTime: TemporalFilter? = null
            private var forSystemTime: TemporalFilter? = null
            private var projectAllCols: Boolean = false

            fun forValidTime(validTime: TemporalFilter?) = this.apply { this.forValidTime = validTime }
            fun forSystemTime(systemTime: TemporalFilter?) = this.apply { this.forSystemTime = systemTime }

            @JvmOverloads
            fun projectAllCols(projectAllCols: Boolean = true) = this.apply { this.projectAllCols = projectAllCols }

            override fun build() = From(table, getBindings(), forValidTime, forSystemTime, projectAllCols)
        }
    }

    data class Where(@JvmField val preds: List<Expr>) : QueryTail, UnifyClause

    data class With(@JvmField val bindings: List<Binding>) : QueryTail, UnifyClause {
        class Builder : Binding.ABuilder<Builder, With>() {
            override fun build() = With(getBindings())
        }
    }

    data class Without(@JvmField val cols: List<String>) : QueryTail

    data class Return(@JvmField val cols: List<Binding>) : QueryTail {
        class Builder : Binding.ABuilder<Builder, Return>() {
            override fun build() = Return(getBindings())
        }
    }

    data class Call(
        @JvmField val ruleName: String,
        @JvmField val args: List<Expr>,
        @JvmField val bindings: List<Binding>? = null,
    ) : UnifyClause {

        fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    interface IJoin : UnifyClause {
        fun binding(bindings: List<Binding>): IJoin
    }

    data class Join(
        @JvmField val query: XtqlQuery,
        @JvmField val args: List<Binding>? = null,
        @JvmField val bindings: List<Binding>? = null,
    ) : IJoin {
        override fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    data class LeftJoin(
        @JvmField val query: XtqlQuery,
        @JvmField val args: List<Binding>? = null,
        @JvmField val bindings: List<Binding>? = null,
    ) : IJoin {

        override fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    data class Aggregate(@JvmField val cols: List<Binding>) : QueryTail {
        class Builder : Binding.ABuilder<Builder, Aggregate>() {
            override fun build() = Aggregate(getBindings())
        }
    }

    enum class OrderDirection {
        ASC, DESC
    }

    enum class OrderNulls {
        FIRST, LAST
    }

    data class OrderSpec(
        @JvmField val expr: Expr,
        @JvmField val direction: OrderDirection? = null,
        @JvmField val nulls: OrderNulls? = null,
    ) {
        fun asc() = copy(direction = ASC)
        fun desc() = copy(direction = DESC)
        fun nullsFirst() = copy(nulls = FIRST)
        fun nullsLast() = copy(nulls = LAST)
    }

    data class OrderBy(@JvmField val orderSpecs: List<OrderSpec?>) : QueryTail

    data class UnionAll(@JvmField val queries: List<XtqlQuery>) : XtqlQuery

    data class Limit(@JvmField val length: Long) : QueryTail

    data class Offset(@JvmField val length: Long) : QueryTail

    abstract class Relation : XtqlQuery, UnifyClause

    data class DocsRelation(
        @JvmField val documents: List<Map<String, Expr>>,
        @JvmField val bindings: List<Binding>,
    ) :
        Relation() {
        fun bindings(bindings: List<Binding>) = copy(bindings = bindings)
    }

    data class ParamRelation(
        @JvmField val param: Param,
        @JvmField val bindings: List<Binding?>,
    ) : Relation()

    data class Unnest(@JvmField val binding: Binding) : QueryTail, UnifyClause
}

object Queries {
    @JvmStatic
    fun pipeline(query: XtqlQuery, tails: List<XtqlQuery.QueryTail>) = XtqlQuery.Pipeline(query, tails)

    @JvmStatic
    fun pipeline(query: XtqlQuery, vararg tails: XtqlQuery.QueryTail) = pipeline(query, tails.toList())

    @JvmStatic
    fun unify(clauses: List<XtqlQuery.UnifyClause>) = XtqlQuery.Unify(clauses)

    @JvmStatic
    fun unify(vararg clauses: XtqlQuery.UnifyClause) = unify(clauses.toList())

    @JvmStatic
    fun from(table: String) = XtqlQuery.From.Builder(table)

    @JvmSynthetic
    fun from(table: String, b: XtqlQuery.From.Builder.() -> Unit) = from(table).also { it.b() }.build()

    @JvmStatic
    fun where(preds: List<Expr>) = XtqlQuery.Where(preds)

    @JvmStatic
    fun where(vararg preds: Expr) = where(preds.toList())

    @JvmStatic
    fun with(vars: List<Binding>) = XtqlQuery.With(vars)

    @JvmStatic
    fun with() = XtqlQuery.With.Builder()

    @JvmSynthetic
    fun with(b: XtqlQuery.With.Builder.() -> Unit) = with().also { it.b() }.build()

    @JvmStatic
    fun without(cols: List<String>) = XtqlQuery.Without(cols)

    @JvmStatic
    fun without(vararg cols: String) = without(cols.toList())

    @JvmStatic
    fun returning(cols: List<Binding>) = XtqlQuery.Return(cols)

    @JvmStatic
    fun returning() = XtqlQuery.Return.Builder()

    @JvmSynthetic
    fun returning(b: XtqlQuery.Return.Builder.() -> Unit) = returning().also { it.b() }.build()

    @JvmStatic
    fun call(ruleName: String, args: List<Expr>) = XtqlQuery.Call(ruleName, args)

    @JvmStatic
    fun call(ruleName: String, vararg args: Expr) = XtqlQuery.Call(ruleName, args.toList())

    @JvmStatic
    fun join(query: XtqlQuery, args: List<Binding>? = null) = XtqlQuery.Join(query, args)

    @JvmStatic
    fun leftJoin(query: XtqlQuery, args: List<Binding>? = null) = XtqlQuery.LeftJoin(query, args)

    @JvmStatic
    fun aggregate(cols: List<Binding>) = XtqlQuery.Aggregate(cols)

    @JvmStatic
    fun aggregate() = XtqlQuery.Aggregate.Builder()

    @JvmSynthetic
    fun aggregate(b: XtqlQuery.Aggregate.Builder.() -> Unit) = aggregate().also { it.b() }.build()

    @JvmStatic
    fun orderSpec(col: String) = orderSpec(lVar(col))

    @JvmStatic
    fun orderSpec(expr: Expr) = XtqlQuery.OrderSpec(expr)

    @JvmStatic
    fun orderSpec(expr: Expr, direction: XtqlQuery.OrderDirection?, nulls: XtqlQuery.OrderNulls?) =
        XtqlQuery.OrderSpec(expr, direction, nulls)

    @JvmStatic
    fun orderBy(orderSpecs: List<XtqlQuery.OrderSpec>) = XtqlQuery.OrderBy(orderSpecs)

    @JvmStatic
    fun orderBy(vararg orderSpecs: XtqlQuery.OrderSpec) = orderBy(orderSpecs.toList())

    @JvmStatic
    fun unionAll(queries: List<XtqlQuery>) = XtqlQuery.UnionAll(queries)

    @JvmStatic
    fun unionAll(vararg queries: XtqlQuery) = unionAll(queries.toList())

    @JvmStatic
    fun limit(length: Long) = XtqlQuery.Limit(length)

    @JvmStatic
    fun offset(length: Long) = XtqlQuery.Offset(length)

    @JvmStatic
    fun relation(documents: List<Map<String, Expr>>, bindings: List<Binding>) =
        XtqlQuery.DocsRelation(documents, bindings)

    @JvmStatic
    fun relation(documents: List<Map<String, Expr>>, vararg bindings: Binding) =
        relation(documents, bindings.toList())

    @JvmStatic
    fun relation(param: Param, bindings: List<Binding>) = XtqlQuery.ParamRelation(param, bindings)

    @JvmStatic
    fun relation(param: Param, vararg bindings: Binding) = relation(param, bindings.toList())

    @JvmStatic
    fun unnest(binding: Binding) = XtqlQuery.Unnest(binding)
}
