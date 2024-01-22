@file:UseSerializers(AnySerde::class, InstantSerde::class)
package xtdb.api.tx

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.json.JsonContentPolymorphicSerializer
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonObject
import xtdb.AnySerde
import xtdb.InstantSerde
import xtdb.api.query.Binding
import xtdb.api.query.TemporalFilter.TemporalExtents
import xtdb.api.query.XtqlQuery
import xtdb.api.query.XtqlQuery.UnifyClause
import xtdb.jsonIAE
import xtdb.types.ClojureForm
import java.nio.ByteBuffer
import java.time.Instant

private const val XT_TXS = "xt/tx_fns"
private const val XT_ID = "xt/id"
private const val XT_FN = "xt/fn"

@Serializable(TxOp.Serde::class)
sealed interface TxOp {
    object Serde : JsonContentPolymorphicSerializer<TxOp>(TxOp::class) {
        override fun selectDeserializer(element: JsonElement): DeserializationStrategy<TxOp> = when {
            "putDocs" in element.jsonObject -> PutDocs.serializer()
            "deleteDocs" in element.jsonObject -> DeleteDocs.serializer()
            "eraseDocs" in element.jsonObject -> EraseDocs.serializer()
            "call" in element.jsonObject -> Call.serializer()
            "sql" in element.jsonObject -> Sql.serializer()
            "op" in element.jsonObject -> XtqlAndArgs.serializer()
            "insertInto" in element.jsonObject ->  Xtql.Insert.serializer()
            "update" in element.jsonObject -> Xtql.Update.serializer()
            "deleteFrom" in element.jsonObject -> Xtql.Delete.serializer()
            "eraseFrom" in element.jsonObject -> Xtql.Erase.serializer()
            "assertExists" in element.jsonObject -> Xtql.AssertExists.serializer()
            "assertNotExists" in element.jsonObject -> Xtql.AssertNotExists.serializer()
            else -> throw jsonIAE("xtql/malformed-tx-op", element)
        }
    }

    companion object {
        @JvmStatic
        fun sql(sql: String) = Sql(sql)

        @JvmStatic
        fun sql(sql: String, argBytes: ByteBuffer) = SqlByteArgs(sql, argBytes)

        @JvmStatic
        fun putDocs(tableName: String, docs: List<Map<String, *>>) = PutDocs(tableName, docs)

        @JvmStatic
        @SafeVarargs
        fun putDocs(tableName: String, vararg docs: Map<String, *>) = putDocs(tableName, docs.toList())

        @JvmStatic
        fun putFn(fnId: Any, fnForm: Any) = putDocs(XT_TXS, listOf(mapOf(XT_ID to fnId, XT_FN to ClojureForm(fnForm))))

        @JvmStatic
        fun deleteDocs(tableName: String, docIds: List<*>) = DeleteDocs(tableName, docIds)

        @JvmStatic
        fun deleteDocs(tableName: String, vararg entityIds: Any) = DeleteDocs(tableName, entityIds.toList())

        @JvmStatic
        fun eraseDocs(tableName: String, entityIds: List<*>) = EraseDocs(tableName, entityIds)

        @JvmStatic
        fun eraseDocs(tableName: String, vararg entityIds: Any) = EraseDocs(tableName, entityIds.toList())

        @JvmStatic
        fun call(fnId: Any, args: List<Any>) = Call(fnId, args)

        @JvmField
        val ABORT = Abort
    }
}

@Serializable
data class PutDocs(
    @JvmField @SerialName("into") val tableName: String,
    @JvmField @SerialName("putDocs") val docs: List<Map<String, *>>,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null
) : TxOp {

    fun startingFrom(validFrom: Instant?) = copy(validFrom = validFrom)
    fun until(validTo: Instant?) = copy(validTo = validTo)
    fun during(validFrom: Instant?, validTo: Instant?) = copy(validFrom = validFrom, validTo = validTo)
}

@Serializable
data class DeleteDocs(
    @JvmField @SerialName("from") val tableName: String,
    @JvmField @SerialName("deleteDocs") val docIds: List<*>,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null
) : TxOp {

    fun startingFrom(validFrom: Instant?) = copy(validFrom = validFrom)
    fun until(validTo: Instant?) = copy(validTo = validTo)
    fun during(validFrom: Instant?, validTo: Instant?) = copy(validFrom = validFrom, validTo = validTo)
}

@Serializable
data class EraseDocs(
    @JvmField @SerialName("from") val tableName: String,
    @JvmField @SerialName("eraseDocs") val docIds: List<*>,
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

@Serializable
data class XtqlAndArgs(
    @JvmField val op: Xtql,
    @JvmField val argRows: List<Map<String, *>>? = null
) : TxOp {

    fun argRows(argRows : List<Map<String, *>>?) = copy(argRows = argRows)
    fun argRows(vararg argRows : Map<String, *>) = argRows(argRows.toList())
}

@Serializable(Xtql.Serde::class)
sealed interface Xtql {

    object Serde : JsonContentPolymorphicSerializer<Xtql>(Xtql::class) {
        override fun selectDeserializer(element: JsonElement): DeserializationStrategy<Xtql> = when {
            "insertInto" in element.jsonObject ->  Insert.serializer()
            "update" in element.jsonObject -> Update.serializer()
            "deleteFrom" in element.jsonObject -> Delete.serializer()
            "eraseFrom" in element.jsonObject -> Erase.serializer()
            "assertExists" in element.jsonObject -> AssertExists.serializer()
            "assertNotExists" in element.jsonObject -> AssertNotExists.serializer()
            else -> throw jsonIAE("xtql/malformed-xtql-op", element)
        }
    }

    fun argRows(argRows : List<Map<String, *>>?) = XtqlAndArgs(this, argRows)
    fun argRows(vararg argRows : Map<String, *>) = argRows(argRows.toList())

    @Serializable
    data class Insert(
        @JvmField @SerialName("insertInto") val table: String,
        @JvmField val query: XtqlQuery
    ) : TxOp, Xtql

    @Serializable
    data class Update(
        @JvmField @SerialName("update") val table: String,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField @SerialName("bind") val bindSpecs: List<Binding>? = null,
        @JvmField @SerialName("set") val setSpecs: List<Binding>,
        @JvmField @SerialName("unify") val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>) = copy(unifyClauses = unifyClauses)
    }

    @Serializable
    data class Delete(
        @JvmField @SerialName("deleteFrom") val table: String,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField @SerialName("bind") val bindSpecs: List<Binding>? = null,
        @JvmField @SerialName("unify") val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = copy(unifyClauses = unifyClauses)
    }

    @Serializable
    data class Erase(
        @JvmField @SerialName("eraseFrom") val table: String,
        @JvmField @SerialName("bind") val bindSpecs: List<Binding>? = null,
        @JvmField @SerialName("unify") val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = Erase(table, bindSpecs, unifyClauses)
    }

    @Serializable
    data class AssertExists(@JvmField @SerialName("assertExists") val query: XtqlQuery) : TxOp, Xtql

    @Serializable
    data class AssertNotExists(@JvmField @SerialName("assertNotExists") val query: XtqlQuery) : TxOp, Xtql

    companion object {
        @JvmStatic
        fun insert(table: String, query: XtqlQuery): Insert = Insert(table, query)

        @JvmStatic
        fun update(table: String, setSpecs: List<Binding>) = Update(table, setSpecs = setSpecs)

        @JvmStatic
        fun delete(table: String): Delete = Delete(table)

        @JvmStatic
        fun erase(table: String): Erase = Erase(table)

        @JvmStatic
        fun assertExists(query: XtqlQuery) = AssertExists(query)

        @JvmStatic
        fun assertNotExists(query: XtqlQuery) = AssertNotExists(query)
    }
}

@Serializable
data class Call(
    @JvmField @SerialName("call") val fnId: Any,
    @JvmField val args: List<Any>
) : TxOp

data object Abort : TxOp
