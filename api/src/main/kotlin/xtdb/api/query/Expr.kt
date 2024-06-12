@file:UseSerializers(AnySerde::class)

package xtdb.api.query

import kotlinx.serialization.UseSerializers
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import xtdb.AnySerde
import xtdb.api.query.Expr.Null
import xtdb.jsonIAE
import kotlin.Double as KDouble
import kotlin.Long as KLong

private fun JsonElement.requireObject(errorType: String) = this as? JsonObject ?: throw jsonIAE(errorType, this)
private fun JsonElement.requireArray(errorType: String) = this as? JsonArray ?: throw jsonIAE(errorType, this)

private fun JsonObject.requireType(errorType: String) = apply { if ("@type" !in this) throw jsonIAE(errorType, this) }

private fun JsonObject.requireValue(errorType: String) = this["@value"] ?: throw jsonIAE(errorType, this)

sealed interface Expr {

    data object Null : Expr

    enum class Bool(@JvmField val bool: Boolean) : Expr {
        TRUE(true), FALSE(false);
    }

    data class Long(@JvmField val lng: KLong) : Expr
    data class Double(@JvmField val dbl: KDouble) : Expr
    data class Obj(@JvmField val obj: Any) : Expr

    data class LogicVar(@JvmField val lv: String) : Expr
    data class Param(@JvmField val v: String) : Expr

    data class Call(@JvmField val f: String, @JvmField val args: List<Expr>) : Expr

    data class Get(@JvmField val expr: Expr, @JvmField val field: String) : Expr

    data class Subquery(@JvmField val query: XtqlQuery, @JvmField val args: List<Binding>? = null) : Expr
    data class Exists(@JvmField val query: XtqlQuery, @JvmField val args: List<Binding>? = null) : Expr
    data class Pull(@JvmField val query: XtqlQuery, @JvmField val args: List<Binding>? = null) : Expr
    data class PullMany(@JvmField val query: XtqlQuery, @JvmField val args: List<Binding>? = null) : Expr

    data class ListExpr(@JvmField val elements: List<Expr>) : Expr
    data class SetExpr(@JvmField val elements: List<Expr>) : Expr
    data class MapExpr(@JvmField val elements: Map<String, Expr>) : Expr
}

object Exprs {
    @JvmStatic
    fun `val`(l: KLong) = Expr.Long(l)

    @JvmStatic
    fun `val`(d: KDouble) = Expr.Double(d)

    @JvmStatic
    fun `val`(obj: Any) = Expr.Obj(obj)

    @JvmStatic
    fun lVar(lv: String) = Expr.LogicVar(lv)

    @JvmStatic
    fun param(v: String) = Expr.Param(v)

    @JvmStatic
    fun call(f: String, args: List<Expr>) = Expr.Call(f, args)

    @JvmStatic
    fun call(f: String, vararg args: Expr) = call(f, args.toList())

    @JvmStatic
    fun get(expr: Expr, field: String) = Expr.Get(expr, field)

    @JvmStatic
    fun q(query: XtqlQuery, args: List<Binding>? = null) = Expr.Subquery(query, args)

    @JvmStatic
    fun exists(query: XtqlQuery, args: List<Binding>? = null) = Expr.Exists(query, args)

    @JvmStatic
    fun pull(query: XtqlQuery, args: List<Binding>? = null) = Expr.Pull(query, args)

    @JvmStatic
    fun pullMany(query: XtqlQuery, args: List<Binding>? = null) = Expr.PullMany(query, args)

    @JvmStatic
    fun list(elements: List<Expr>) = Expr.ListExpr(elements)

    @JvmStatic
    fun list(vararg elements: Expr) = list(elements.toList())

    @JvmStatic
    fun set(elements: List<Expr>) = Expr.SetExpr(elements)

    @JvmStatic
    fun set(vararg elements: Expr) = set(elements.toList())

    @JvmStatic
    fun map(elements: Map<String, Expr>) = Expr.MapExpr(elements)

    @JvmSynthetic
    fun map(vararg elements: Pair<String, Expr>) = map(elements.toMap())

    class Builder internal constructor(){
        operator fun String.invoke(vararg args: Expr) = call(this, *args)

        val `null` = Null
        val Int.const get() = `val`(this.toLong())
        val KLong.const get() = `val`(this)
        val Float.const get() = `val`(this.toDouble())
        val KDouble.const get() = `val`(this)
        val Any.const get() = `val`(this)
        val String.sym get() = lVar(this)
        val String.param get() = param(this)
        operator fun Expr.get(field: String) = get(this, field)
    }

    @JvmSynthetic
    fun expr(build: Builder.() -> Expr) = Builder().build()
}
