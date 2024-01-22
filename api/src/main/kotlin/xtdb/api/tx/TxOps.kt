@file:JvmName("TxOps")
@file:UseSerializers(AnySerde::class, InstantSerde::class)

package xtdb.api.tx

import kotlinx.serialization.*
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
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
            "argRows" in element.jsonObject -> XtqlAndArgs.serializer()
            else -> XtqlOp.serializer()
        }
    }
}

@Serializable
data class PutDocs(
    @JvmField @SerialName("into") val tableName: String,
    @JvmField @SerialName("putDocs") val docs: List<Map<String, *>>,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null,
) : TxOp {

    fun startingFrom(validFrom: Instant?) = copy(validFrom = validFrom)
    fun until(validTo: Instant?) = copy(validTo = validTo)
    fun during(validFrom: Instant?, validTo: Instant?) = copy(validFrom = validFrom, validTo = validTo)
}

fun putDocs(tableName: String, docs: List<Map<String, *>>) = PutDocs(tableName, docs)

@SafeVarargs
fun putDocs(tableName: String, vararg docs: Map<String, *>) = putDocs(tableName, docs.toList())

fun putFn(fnId: Any, fnForm: Any) = putDocs(XT_TXS, listOf(mapOf(XT_ID to fnId, XT_FN to ClojureForm(fnForm))))

@Serializable
data class DeleteDocs(
    @JvmField @SerialName("from") val tableName: String,
    @JvmField @SerialName("deleteDocs") val docIds: List<*>,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null,
) : TxOp {

    fun startingFrom(validFrom: Instant?) = copy(validFrom = validFrom)
    fun until(validTo: Instant?) = copy(validTo = validTo)
    fun during(validFrom: Instant?, validTo: Instant?) = copy(validFrom = validFrom, validTo = validTo)
}

fun deleteDocs(tableName: String, docIds: List<*>) = DeleteDocs(tableName, docIds)

fun deleteDocs(tableName: String, vararg entityIds: Any) = DeleteDocs(tableName, entityIds.toList())

@Serializable
data class EraseDocs(
    @JvmField @SerialName("from") val tableName: String,
    @JvmField @SerialName("eraseDocs") val docIds: List<*>,
) : TxOp

fun eraseDocs(tableName: String, entityIds: List<*>) = EraseDocs(tableName, entityIds)

fun eraseDocs(tableName: String, vararg entityIds: Any) = EraseDocs(tableName, entityIds.toList())

@Serializable
data class Sql(
    @JvmField @SerialName("sql") val sql: String,
    @JvmField val argRows: List<List<*>>? = null,
) : TxOp {

    fun argRows(argRows: List<List<*>>?): Sql = Sql(sql, argRows = argRows)
}

fun sql(sql: String) = Sql(sql)

fun sql(sql: String, argBytes: ByteBuffer) = SqlByteArgs(sql, argBytes)

data class SqlByteArgs(
    @JvmField val sql: String,
    @JvmField val argBytes: ByteBuffer? = null,
) : TxOp

@Serializable(XtqlAndArgs.Serde::class)
data class XtqlAndArgs(
    @JvmField val op: XtqlOp,
    @JvmField val argRows: List<Map<String, *>>? = null,
) : TxOp {

    object Serde : KSerializer<XtqlAndArgs> {
        // TODO add argRows
        override val descriptor = XtqlOp.serializer().descriptor

        override fun deserialize(decoder: Decoder): XtqlAndArgs {
            require(decoder is JsonDecoder)
            val jsonElement = decoder.decodeJsonElement().jsonObject
            return XtqlAndArgs(
                decoder.json.decodeFromJsonElement<XtqlOp>(JsonObject(jsonElement - "argRows")),
                decoder.json.decodeFromJsonElement<List<Map<String, Any>>?>(jsonElement["argRows"]!!)
            )
        }

        override fun serialize(encoder: Encoder, value: XtqlAndArgs) {
            require(encoder is JsonEncoder)
            val op = encoder.json.encodeToJsonElement(value.op)

            @Suppress("UNCHECKED_CAST") val argRows = encoder.json.encodeToJsonElement(value.argRows as List<Map<String, Any>>?)

            return encoder.encodeJsonElement(JsonObject(op.jsonObject + ("argRows" to argRows)))
        }
    }

    fun argRows(argRows: List<Map<String, *>>?) = copy(argRows = argRows)
    fun argRows(vararg argRows: Map<String, *>) = argRows(argRows.toList())
}

@Serializable(XtqlOp.Serde::class)
sealed interface XtqlOp : TxOp {

    object Serde : JsonContentPolymorphicSerializer<XtqlOp>(XtqlOp::class) {
        override fun selectDeserializer(element: JsonElement): DeserializationStrategy<XtqlOp> = when {
            "insertInto" in element.jsonObject -> Insert.serializer()
            "update" in element.jsonObject -> Update.serializer()
            "deleteFrom" in element.jsonObject -> Delete.serializer()
            "eraseFrom" in element.jsonObject -> Erase.serializer()
            "assertExists" in element.jsonObject -> AssertExists.serializer()
            "assertNotExists" in element.jsonObject -> AssertNotExists.serializer()
            else -> throw jsonIAE("xtql/malformed-tx-op", element)
        }
    }

    fun argRows(argRows: List<Map<String, *>>?) = XtqlAndArgs(this, argRows)
    fun argRows(vararg argRows: Map<String, *>) = argRows(argRows.toList())
}

@Serializable
data class Insert(
    @JvmField @SerialName("insertInto") val table: String,
    @JvmField val query: XtqlQuery,
) : XtqlOp

fun insert(table: String, query: XtqlQuery): Insert = Insert(table, query)

@Serializable
data class Update(
    @JvmField @SerialName("update") val table: String,
    @JvmField val forValidTime: TemporalExtents? = null,
    @JvmField @SerialName("bind") val bindSpecs: List<Binding>? = null,
    @JvmField @SerialName("set") val setSpecs: List<Binding>,
    @JvmField @SerialName("unify") val unifyClauses: List<UnifyClause>? = null,
) : XtqlOp {

    fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
    fun binding(bindSpecs: List<Binding>) = copy(bindSpecs = bindSpecs)
    fun unify(unifyClauses: List<UnifyClause>) = copy(unifyClauses = unifyClauses)
}

fun update(table: String, setSpecs: List<Binding>) = Update(table, setSpecs = setSpecs)

@Serializable
data class Delete(
    @JvmField @SerialName("deleteFrom") val table: String,
    @JvmField val forValidTime: TemporalExtents? = null,
    @JvmField @SerialName("bind") val bindSpecs: List<Binding>? = null,
    @JvmField @SerialName("unify") val unifyClauses: List<UnifyClause>? = null,
) : XtqlOp {

    fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
    fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
    fun unify(unifyClauses: List<UnifyClause>?) = copy(unifyClauses = unifyClauses)
}

fun delete(table: String): Delete = Delete(table)

@Serializable
data class Erase(
    @JvmField @SerialName("eraseFrom") val table: String,
    @JvmField @SerialName("bind") val bindSpecs: List<Binding>? = null,
    @JvmField @SerialName("unify") val unifyClauses: List<UnifyClause>? = null,
) : XtqlOp {

    fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
    fun unify(unifyClauses: List<UnifyClause>?) = Erase(table, bindSpecs, unifyClauses)
}

fun erase(table: String): Erase = Erase(table)

@Serializable
data class AssertExists(@JvmField @SerialName("assertExists") val query: XtqlQuery) : XtqlOp

fun assertExists(query: XtqlQuery) = AssertExists(query)

@Serializable
data class AssertNotExists(@JvmField @SerialName("assertNotExists") val query: XtqlQuery) : XtqlOp

fun assertNotExists(query: XtqlQuery) = AssertNotExists(query)

@Serializable
data class Call(
    @JvmField @SerialName("call") val fnId: Any,
    @JvmField val args: List<Any>,
) : TxOp

fun call(fnId: Any, args: List<Any>) = Call(fnId, args)

data object Abort : TxOp

@JvmField
val ABORT = Abort

