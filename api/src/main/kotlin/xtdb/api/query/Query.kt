package xtdb.api.query

import xtdb.api.query.Expr.Companion.lVar
import xtdb.api.query.Expr.Param
import xtdb.api.query.Query.OrderDirection.ASC
import xtdb.api.query.Query.OrderDirection.DESC
import xtdb.api.query.Query.OrderNulls.FIRST
import xtdb.api.query.Query.OrderNulls.LAST

sealed interface Query {
    sealed interface QueryTail
    sealed interface UnifyClause

    data class Pipeline(@JvmField val query: Query, @JvmField val tails: List<QueryTail>) : Query

    data class Unify(@JvmField val clauses: List<UnifyClause>) : Query

    data class From(
        @JvmField val table: String,
        @JvmField val bindings: List<Binding>,
        @JvmField val forValidTime: TemporalFilter? = null,
        @JvmField val forSystemTime: TemporalFilter? = null,
        @JvmField val projectAllCols: Boolean = false,
    ) : Query, UnifyClause {

        constructor(table: String, bindings: List<Binding>) : this(table, bindings, null, null, false)

        class Builder(private val table: String) : Binding.ABuilder<Builder, From>() {
            private var forValidTime: TemporalFilter? = null
            private var forSystemTime: TemporalFilter? = null
            private var projectAllCols: Boolean = false

            fun forValidTime(validTime: TemporalFilter?) = this.apply { this.forValidTime = validTime }
            fun forSystemTime(systemTime: TemporalFilter?) = this.apply { this.forSystemTime = systemTime }

            @JvmOverloads
            fun projectAllCols(projectAllCols: Boolean = true) = this.apply { this.projectAllCols = projectAllCols }

            override fun build() = From(table, buildBindings(), forValidTime, forSystemTime, projectAllCols)
        }
    }

    data class Where(@JvmField val preds: List<Expr>) : QueryTail, UnifyClause

    data class With(@JvmField val vars: List<Binding>) : UnifyClause {
        class Builder : Binding.ABuilder<Builder, With>() {
            override fun build() = With(buildBindings())
        }
    }

    data class WithCols(@JvmField val cols: List<Binding>) : QueryTail {
        class Builder : Binding.ABuilder<Builder, WithCols>() {
            override fun build() = WithCols(buildBindings())
        }
    }

    data class Without(@JvmField val cols: List<String>) : QueryTail

    data class Return(@JvmField val cols: List<Binding>) : QueryTail {
        class Builder : Binding.ABuilder<Builder, Return>() {
            override fun build() = Return(buildBindings())
        }
    }

    data class Call(
        @JvmField val ruleName: String, @JvmField val args: List<Expr>, @JvmField val bindings: List<Binding>? = null,
    ) : UnifyClause {

        fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    interface IJoin : UnifyClause {
        fun binding(bindings: List<Binding>): IJoin
    }

    data class Join(
        @JvmField val query: Query,
        @JvmField val args: List<Binding>? = null,
        @JvmField val bindings: List<Binding>? = null,
    ) : IJoin {
        override fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    data class LeftJoin(
        @JvmField val query: Query,
        @JvmField val args: List<Binding>? = null,
        @JvmField val bindings: List<Binding>? = null,
    ) : IJoin {

        override fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    data class Aggregate(@JvmField val cols: List<Binding>) : QueryTail {
        class Builder : Binding.ABuilder<Builder, Aggregate>() {
            override fun build() = Aggregate(buildBindings())
        }
    }

    enum class OrderDirection { ASC, DESC }
    enum class OrderNulls { FIRST, LAST }

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

    data class UnionAll(@JvmField val queries: List<Query>) : Query

    data class Limit(@JvmField val length: Long) : QueryTail
    data class Offset(@JvmField val length: Long) : QueryTail

    abstract class Relation : Query, UnifyClause

    data class DocsRelation(@JvmField val documents: List<Map<String, Expr>>, @JvmField val bindings: List<Binding>) :
        Relation() {
        fun bindings(bindings: List<Binding>) = copy(bindings = bindings)
    }

    data class ParamRelation(@JvmField val param: Param, @JvmField val bindings: List<Binding?>) : Relation()

    data class UnnestVar(@JvmField val `var`: Binding) : UnifyClause
    data class UnnestCol(@JvmField val col: Binding) : QueryTail

    companion object {
        @JvmStatic
        fun pipeline(query: Query, tails: List<QueryTail>) = Pipeline(query, tails)

        @JvmStatic
        fun pipeline(query: Query, vararg tails: QueryTail) = pipeline(query, tails.toList())

        @JvmStatic
        fun unify(clauses: List<UnifyClause>) = Unify(clauses)

        @JvmStatic
        fun unify(vararg clauses: UnifyClause) = unify(clauses.toList())

        @JvmStatic
        fun from(table: String) = From.Builder(table)

        @JvmSynthetic
        fun from(table: String, b: From.Builder.() -> Unit) = from(table).also { it.b() }.build()

        @JvmStatic
        fun where(preds: List<Expr>) = Where(preds)

        @JvmStatic
        fun where(vararg preds: Expr) = where(preds.toList())

        @JvmStatic
        fun with(vars: List<Binding>) = With(vars)

        @JvmStatic
        fun with() = With.Builder()

        @JvmSynthetic
        fun with(b: With.Builder.() -> Unit) = with().also { it.b() }.build()

        @JvmStatic
        fun withCols(cols: List<Binding>) = WithCols(cols)

        @JvmStatic
        fun withCols() = WithCols.Builder()

        @JvmSynthetic
        fun withCols(b: WithCols.Builder.() -> Unit) = withCols().also { it.b() }.build()

        @JvmStatic
        fun without(cols: List<String>) = Without(cols)

        @JvmStatic
        fun without(vararg cols: String) = without(cols.toList())

        @JvmStatic
        fun returning(cols: List<Binding>) = Return(cols)

        @JvmStatic
        fun returning() = Return.Builder()

        @JvmSynthetic
        fun returning(b: Return.Builder.() -> Unit) = returning().also { it.b() }.build()

        @JvmStatic
        fun call(ruleName: String, args: List<Expr>) = Call(ruleName, args)

        @JvmStatic
        fun call(ruleName: String, vararg args: Expr) = Call(ruleName, args.toList())

        @JvmStatic
        fun join(query: Query, args: List<Binding>?) = Join(query, args)

        @JvmStatic
        fun leftJoin(query: Query, args: List<Binding>?) = LeftJoin(query, args)

        @JvmStatic
        fun aggregate(cols: List<Binding>) = Aggregate(cols)

        @JvmStatic
        fun aggregate() = Aggregate.Builder()

        @JvmSynthetic
        fun aggregate(b: Aggregate.Builder.() -> Unit) = aggregate().also { it.b() }.build()

        @JvmStatic
        fun orderSpec(col: String) = orderSpec(lVar(col))

        @JvmStatic
        fun orderSpec(expr: Expr) = OrderSpec(expr)

        @JvmStatic
        fun orderSpec(expr: Expr, direction: OrderDirection?, nulls: OrderNulls?) = OrderSpec(expr, direction, nulls)

        @JvmStatic
        fun orderBy(orderSpecs: List<OrderSpec>) = OrderBy(orderSpecs)

        @JvmStatic
        fun orderBy(vararg orderSpecs: OrderSpec) = orderBy(orderSpecs.toList())

        @JvmStatic
        fun unionAll(queries: List<Query>) = UnionAll(queries)

        @JvmStatic
        fun unionAll(vararg queries: Query) = unionAll(queries.toList())

        @JvmStatic
        fun limit(length: Long) = Limit(length)

        @JvmStatic
        fun offset(length: Long) = Offset(length)

        @JvmStatic
        fun relation(documents: List<Map<String, Expr>>, bindings: List<Binding>) = DocsRelation(documents, bindings)

        @JvmStatic
        fun relation(documents: List<Map<String, Expr>>, vararg bindings: Binding) =
            relation(documents, bindings.toList())

        @JvmStatic
        fun relation(param: Param, bindings: List<Binding>) = ParamRelation(param, bindings)

        @JvmStatic
        fun relation(param: Param, vararg bindings: Binding) = relation(param, bindings.toList())

        @JvmStatic
        fun unnestVar(`var`: Binding) = UnnestVar(`var`)

        @JvmStatic
        fun unnestCol(col: Binding) = UnnestCol(col)
    }
}
