package xtdb.api.query

sealed interface TemporalFilter {
    sealed interface TemporalExtents : TemporalFilter {
        val from: Expr?
        val to: Expr?
    }

    object AllTime : TemporalFilter, TemporalExtents {
        override val from = null
        override val to = null
    }

    data class At(val at: Expr) : TemporalFilter

    data class In(override val from: Expr?, override val to: Expr?) : TemporalFilter, TemporalExtents

    companion object {
        @JvmStatic
        fun at(atExpr: Expr) = At(atExpr)

        @JvmStatic
        fun `in`(fromExpr: Expr?, toExpr: Expr?) = In(fromExpr, toExpr)

        @JvmStatic
        fun from(fromExpr: Expr?) = In(fromExpr, null)

        @JvmStatic
        fun to(toExpr: Expr?) = In(null, toExpr)

        @JvmField
        val ALL_TIME = AllTime
    }
}
