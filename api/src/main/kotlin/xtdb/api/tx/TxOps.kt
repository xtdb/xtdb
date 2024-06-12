@file:UseSerializers(AnySerde::class, InstantSerde::class)

package xtdb.api.tx

import clojure.lang.Keyword
import kotlinx.serialization.*
import kotlinx.serialization.json.*
import xtdb.AnySerde
import xtdb.IllegalArgumentException
import xtdb.InstantSerde
import xtdb.api.query.Binding
import xtdb.api.query.TemporalFilter.TemporalExtents
import xtdb.api.query.XtqlQuery
import xtdb.api.query.XtqlQuery.UnifyClause
import xtdb.jsonIAE
import xtdb.types.ClojureForm
import xtdb.util.normalForm
import java.nio.ByteBuffer
import java.time.Instant

private const val XT_TXS = "xt/tx_fns"
private const val XT_ID = "xt/id"
private const val XT_FN = "xt/fn"

sealed interface TxOp {
    data class PutDocs(
        @JvmField val tableName: String,
        @JvmField val docs: List<Map<String, *>>,
        @JvmField val validFrom: Instant? = null,
        @JvmField val validTo: Instant? = null,
    ) : TxOp {

        fun startingFrom(validFrom: Instant?) = copy(validFrom = validFrom)
        fun until(validTo: Instant?) = copy(validTo = validTo)
        fun during(validFrom: Instant?, validTo: Instant?) = copy(validFrom = validFrom, validTo = validTo)
    }

    data class DeleteDocs(
        @JvmField val tableName: String,
        @JvmField val docIds: List<*>,
        @JvmField val validFrom: Instant? = null,
        @JvmField val validTo: Instant? = null,
    ) : TxOp {

        fun startingFrom(validFrom: Instant?) = copy(validFrom = validFrom)
        fun until(validTo: Instant?) = copy(validTo = validTo)
        fun during(validFrom: Instant?, validTo: Instant?) = copy(validFrom = validFrom, validTo = validTo)
    }

    data class EraseDocs(
        @JvmField val tableName: String,
        @JvmField val docIds: List<*>,
    ) : TxOp

    @Serializable
    data class Sql(
        @JvmField @SerialName("sql") val sql: String,
        @JvmField val argRows: List<List<*>>? = null,
    ) : TxOp {

        fun argRows(argRows: List<List<*>>?): Sql = Sql(sql, argRows = argRows)
    }

    data class SqlByteArgs(
        @JvmField val sql: String,
        @JvmField val argBytes: ByteBuffer? = null,
    ) : TxOp

    data class XtqlAndArgs(
        @JvmField val op: XtqlOp,
        @JvmField val argRows: List<Map<String, *>>? = null,
    ) : TxOp {

        fun argRows(argRows: List<Map<String, *>>?) = copy(argRows = argRows)
        fun argRows(vararg argRows: Map<String, *>) = argRows(argRows.toList())
    }

    sealed interface XtqlOp : TxOp {

        fun argRows(argRows: List<Map<String, *>>?) = XtqlAndArgs(this, argRows)
        fun argRows(vararg argRows: Map<String, *>) = argRows(argRows.toList())
    }

    data class Insert(
        @JvmField val table: String,
        @JvmField val query: XtqlQuery,
    ) : XtqlOp

    data class Update(
        @JvmField val table: String,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val setSpecs: List<Binding>,
        @JvmField val unifyClauses: List<UnifyClause>? = null,
    ) : XtqlOp {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>) = copy(unifyClauses = unifyClauses)
    }

    data class Delete(
        @JvmField val table: String,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null,
    ) : XtqlOp {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = copy(unifyClauses = unifyClauses)
    }

    data class Erase(
        @JvmField val table: String,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null,
    ) : XtqlOp {

        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = Erase(table, bindSpecs, unifyClauses)
    }

    data class AssertExists(@JvmField val query: XtqlQuery) : XtqlOp

    data class AssertNotExists(@JvmField val query: XtqlQuery) : XtqlOp

    data class Call(
        @JvmField val fnId: Any,
        @JvmField val args: List<Any>,
    ) : TxOp

    data object Abort : TxOp
}

object TxOps {
    private val forbiddenSetColumns = setOf("xt\$id", "xt\$valid_from", "xt\$valid_to", "xt\$system_from", "xt\$system_to")

    @JvmStatic
    fun putDocs(tableName: String, docs: List<Map<String, *>>) = TxOp.PutDocs(tableName, docs)

    @JvmStatic
    @SafeVarargs
    fun putDocs(tableName: String, vararg docs: Map<String, *>) = putDocs(tableName, docs.toList())

    @JvmStatic
    fun putFn(fnId: Any, fnForm: Any) = putDocs(XT_TXS, listOf(mapOf(XT_ID to fnId, XT_FN to ClojureForm(fnForm))))

    @JvmStatic
    fun deleteDocs(tableName: String, docIds: List<*>) = TxOp.DeleteDocs(tableName, docIds)

    @JvmStatic
    fun deleteDocs(tableName: String, vararg entityIds: Any) = TxOp.DeleteDocs(tableName, entityIds.toList())

    @JvmStatic
    fun eraseDocs(tableName: String, entityIds: List<*>) = TxOp.EraseDocs(tableName, entityIds)

    @JvmStatic
    fun eraseDocs(tableName: String, vararg entityIds: Any) = TxOp.EraseDocs(tableName, entityIds.toList())

    @JvmStatic
    fun sql(sql: String) = TxOp.Sql(sql)

    @JvmStatic
    fun sql(sql: String, argBytes: ByteBuffer) = TxOp.SqlByteArgs(sql, argBytes)

    @JvmStatic
    fun insert(table: String, query: XtqlQuery) = TxOp.Insert(table, query)

    @JvmStatic
    fun update(table: String, setSpecs: List<Binding>): TxOp.Update {
        if (forbiddenSetColumns.intersect(setSpecs.map { normalForm(it.binding) }.toSet()).isNotEmpty()) {
            throw IllegalArgumentException.createNoKey("Invalid set column for update", mapOf(Keyword.intern("set") to setSpecs))
        } else {
            return TxOp.Update(table, setSpecs = setSpecs)
        }
    }

    @JvmStatic
    fun delete(table: String) = TxOp.Delete(table)

    @JvmStatic
    fun erase(table: String) = TxOp.Erase(table)

    @JvmStatic
    fun assertExists(query: XtqlQuery) = TxOp.AssertExists(query)

    @JvmStatic
    fun assertNotExists(query: XtqlQuery) = TxOp.AssertNotExists(query)

    @JvmStatic
    fun call(fnId: Any, args: List<Any>) = TxOp.Call(fnId, args)

    @JvmField
    val abort = TxOp.Abort
}
