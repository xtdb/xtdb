package xtdb.query

import xtdb.query.Expr.Param
import xtdb.query.Query.OrderDirection.ASC
import xtdb.query.Query.OrderNulls.FIRST
import xtdb.query.Query.OrderNulls.LAST

sealed interface Query {
    sealed interface QueryTail
    sealed interface UnifyClause

    data class Pipeline(@JvmField val query: Query, @JvmField val tails: List<QueryTail>) : Query

    data class Unify(@JvmField val clauses: List<UnifyClause>) : Query

    data class From(
        @JvmField val table: String,
        @JvmField val forValidTime: TemporalFilter? = null,
        @JvmField val forSystemTime: TemporalFilter? = null,
        @JvmField val bindings: List<Binding>? = null,
        @JvmField val projectAllCols: Boolean = false
    ) : Query, UnifyClause {

        fun forValidTime(forValidTime: TemporalFilter) = copy(forValidTime = forValidTime)
        fun forSystemTime(forSystemTime: TemporalFilter) = copy(forSystemTime = forSystemTime)
        fun projectAllCols(projectAllCols: Boolean) = copy(projectAllCols = projectAllCols)

        fun binding(bindings: List<Binding>?) = copy(bindings = bindings)
    }

    data class Where(@JvmField val preds: List<Expr>) : QueryTail, UnifyClause
    data class With(@JvmField val vars: List<Binding>) : UnifyClause
    data class WithCols(@JvmField val cols: List<Binding>) : QueryTail
    data class Without(@JvmField val cols: List<String>) : QueryTail
    data class Return(@JvmField val cols: List<Binding>) : QueryTail

    data class Call(
        @JvmField val ruleName: String, @JvmField val args: List<Expr>, @JvmField val bindings: List<Binding>? = null
    ) : UnifyClause {

        fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    interface IJoin : UnifyClause {
        fun binding(bindings: List<Binding>): IJoin
    }

    data class Join(
        @JvmField val query: Query,
        @JvmField val args: List<Binding>? = null,
        @JvmField val bindings: List<Binding>? = null
    ) : IJoin {
        override fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    data class LeftJoin(
        @JvmField val query: Query,
        @JvmField val args: List<Binding>? = null,
        @JvmField val bindings: List<Binding>? = null
    ) : IJoin {

        override fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    data class Aggregate(@JvmField val cols: List<Binding>) : QueryTail

    enum class OrderDirection { ASC, DESC }
    enum class OrderNulls { FIRST, LAST }

    data class OrderSpec(
        @JvmField val expr: Expr,
        @JvmField val direction: OrderDirection? = null,
        @JvmField val nulls: OrderNulls? = null
    )

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
        fun unify(clauses: List<UnifyClause>) = Unify(clauses)

        @JvmStatic
        fun from(table: String) = From(table)

        @JvmStatic
        fun where(preds: List<Expr>) = Where(preds)

        @JvmStatic
        fun with(vars: List<Binding>) = With(vars)

        @JvmStatic
        fun withCols(cols: List<Binding>) = WithCols(cols)

        @JvmStatic
        fun without(cols: List<String>) = Without(cols)

        @JvmStatic
        fun returning(cols: List<Binding>) = Return(cols)

        @JvmStatic
        fun call(ruleName: String, args: List<Expr>) = Call(ruleName, args)

        @JvmStatic
        fun join(query: Query, args: List<Binding>?) = Join(query, args)

        @JvmStatic
        fun leftJoin(query: Query, args: List<Binding>?) = LeftJoin(query, args)

        @JvmStatic
        fun aggregate(cols: List<Binding>) = Aggregate(cols)

        @JvmStatic
        fun orderSpec(expr: Expr) = OrderSpec(expr)

        @JvmStatic
        fun orderSpec(expr: Expr, direction: OrderDirection?, nulls: OrderNulls?) = OrderSpec(expr, direction, nulls)

        @JvmStatic
        fun orderBy(orderSpecs: List<OrderSpec>) = OrderBy(orderSpecs)

        @JvmStatic
        fun unionAll(queries: List<Query>) = UnionAll(queries)

        @JvmStatic
        fun limit(length: Long) = Limit(length)

        @JvmStatic
        fun offset(length: Long) = Offset(length)

        @JvmStatic
        fun relation(documents: List<Map<String, Expr>>, bindings: List<Binding>) = DocsRelation(documents, bindings)

        @JvmStatic
        fun relation(param: Param, bindings: List<Binding>) = ParamRelation(param, bindings)

        @JvmStatic
        fun unnestVar(`var`: Binding) = UnnestVar(`var`)

        @JvmStatic
        fun unnestCol(col: Binding) = UnnestCol(col)
    }
}
