package xtdb.api.query

import kotlinx.serialization.Serializable
import xtdb.api.query.Exprs.`val`
import java.time.Instant


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
}

object TemporalFilters {

    @JvmField
    val allTime = TemporalFilter.AllTime

    @JvmStatic
    fun at(atExpr: Expr) = TemporalFilter.At(atExpr)

    @JvmStatic
    fun at(at: Instant) = at(`val`(at))

    @JvmStatic
    fun `in`(fromExpr: Expr?, toExpr: Expr?) = TemporalFilter.In(fromExpr, toExpr)

    @JvmStatic
    fun `in`(from: Instant?, to: Instant?) = `in`(from?.let(::`val`), to?.let(::`val`))

    @JvmStatic
    fun from(fromExpr: Expr) = TemporalFilter.In(fromExpr, null)

    @JvmStatic
    fun from(from: Instant) = from(`val`(from))

    @JvmStatic
    fun to(toExpr: Expr?) = TemporalFilter.In(null, toExpr)

    @JvmStatic
    fun to(to: Instant) = to(`val`(to))
}
