package xtdb.api.query

sealed interface Expr {
    data object Null : Expr

    data class Bool(@JvmField val bool: Boolean) : Expr
    data class Long(@JvmField val lng: kotlin.Long) : Expr
    data class Double(@JvmField val dbl: kotlin.Double) : Expr
    data class Obj(@JvmField val obj: Any) : Expr
    data class LogicVar(@JvmField val lv: String) : Expr
    data class Param(@JvmField val v: String) : Expr

    data class Call(@JvmField val f: String, @JvmField val args: List<Expr>) : Expr

    data class Get(@JvmField val expr: Expr, @JvmField val field: String) : Expr

    data class Subquery(@JvmField val query: Query, @JvmField val args: List<Binding>? = null) : Expr
    data class Exists(@JvmField val query: Query, @JvmField val args: List<Binding>? = null) : Expr
    data class Pull(@JvmField val query: Query, @JvmField val args: List<Binding>? = null) : Expr
    data class PullMany(@JvmField val query: Query, @JvmField val args: List<Binding>? = null) : Expr

    data class ListExpr(@JvmField val elements: List<Expr>) : Expr
    data class SetExpr(@JvmField val elements: List<Expr>) : Expr
    data class MapExpr(@JvmField val elements: Map<String, Expr>) : Expr

    companion object {
        @JvmField
        val NULL = Null

        @JvmField
        val TRUE = Bool(true)

        @JvmField
        val FALSE = Bool(false)

        @JvmStatic
        fun `val`(l: kotlin.Long) = Long(l)

        @JvmStatic
        fun `val`(d: kotlin.Double) = Double(d)

        @JvmStatic
        fun `val`(obj: Any) = Obj(obj)

        @JvmStatic
        fun lVar(lv: String) = LogicVar(lv)

        @JvmStatic
        fun param(v: String) = Param(v)

        @JvmStatic
        fun call(f: String, args: List<Expr>) = Call(f, args)

        @JvmStatic
        fun get(expr: Expr, field: String) = Get(expr, field)

        @JvmStatic
        fun q(query: Query, args: List<Binding>? = null) = Subquery(query, args)

        @JvmStatic
        fun exists(query: Query, args: List<Binding>? = null) = Exists(query, args)

        @JvmStatic
        fun pull(query: Query, args: List<Binding>? = null) = Pull(query, args)

        @JvmStatic
        fun pullMany(query: Query, args: List<Binding>? = null) = PullMany(query, args)

        @JvmStatic
        fun list(elements: List<Expr>) = ListExpr(elements)

        @JvmStatic
        fun set(elements: List<Expr>) = SetExpr(elements)

        @JvmStatic
        fun map(elements: Map<String, Expr>) = MapExpr(elements)
    }
}
