@file:UseSerializers(AnySerde::class, InstantSerde::class)
package xtdb.api.tx

import kotlinx.serialization.*
import kotlinx.serialization.json.JsonContentPolymorphicSerializer
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonObject
import xtdb.AnySerde
import xtdb.InstantSerde
import xtdb.api.query.Binding
import xtdb.api.query.Query
import xtdb.api.query.Query.UnifyClause
import xtdb.api.query.TemporalFilter.TemporalExtents
import xtdb.api.tx.TxOp.HasArgs
import xtdb.api.tx.TxOp.HasValidTimeBounds
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
            "put" in element.jsonObject -> Put.serializer()
            "delete" in element.jsonObject -> Delete.serializer()
            "erase" in element.jsonObject -> Erase.serializer()
            "call" in element.jsonObject -> Call.serializer()
            "sql" in element.jsonObject -> Sql.serializer()
            "op" in element.jsonObject -> XtqlAndArgs.serializer()
            "insertInto" in element.jsonObject ->  Xtql.Insert.serializer()
            "updateTable" in element.jsonObject -> Xtql.Update.serializer()
            "deleteFrom" in element.jsonObject -> Xtql.Delete.serializer()
            "eraseFrom" in element.jsonObject -> Xtql.Erase.serializer()
            "assertExists" in element.jsonObject -> Xtql.AssertExists.serializer()
            "assertNotExists" in element.jsonObject -> Xtql.AssertNotExists.serializer()
            else -> throw jsonIAE("xtql/malformed-tx-op", element)
        }
    }


    @Suppress("unused")
    interface HasArgs<ArgType, O : HasArgs<ArgType, O>?> {
        fun withArgs(args: List<ArgType>?): O
    }

    interface HasValidTimeBounds<O : HasValidTimeBounds<O>?> {
        fun startingFrom(validFrom: Instant?): O
        fun until(validTo: Instant?): O
        fun during(validFrom: Instant?, validTo: Instant?): O
    }

    companion object {
        @JvmStatic
        fun sql(sql: String) = Sql(sql)

        @JvmStatic
        fun sql(sql: String, argBytes: ByteBuffer) = SqlByteArgs(sql, argBytes)

        @JvmStatic
        fun put(tableName: String, doc: Map<String, Any>) = Put(tableName, doc)

        @JvmStatic
        fun putFn(fnId: Any, fnForm: Any) = put(XT_TXS, mapOf(XT_ID to fnId, XT_FN to ClojureForm(fnForm)))

        @JvmStatic
        fun delete(tableName: String, entityId: Any) = Delete(tableName, entityId)

        @JvmStatic
        fun erase(tableName: String, entityId: Any) = Erase(tableName, entityId)

        @JvmStatic
        fun call(fnId: Any, args: List<Any>) = Call(fnId, args)

        @JvmField
        val ABORT = Abort
    }
}

@Serializable
data class Put(
    @JvmField @SerialName("put") val tableName: String,
    @JvmField val doc: Map<String, Any>,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null
) : TxOp, HasValidTimeBounds<Put> {

    override fun startingFrom(validFrom: Instant?): Put = Put(tableName, doc, validFrom, validTo)
    override fun until(validTo: Instant?): Put = Put(tableName, doc, validFrom, validTo)
    override fun during(validFrom: Instant?, validTo: Instant?): Put = Put(tableName, doc, validFrom, validTo)
}

@Serializable
data class Delete(
    @JvmField @SerialName("delete") val tableName: String,
    @JvmField @SerialName("id") val entityId: Any,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null
) : TxOp, HasValidTimeBounds<Delete> {

    override fun startingFrom(validFrom: Instant?): Delete = Delete(tableName, entityId, validFrom, validTo)
    override fun until(validTo: Instant?): Delete = Delete(tableName, entityId, validFrom, validTo)
    override fun during(validFrom: Instant?, validTo: Instant?) = Delete(tableName, entityId, validFrom, validTo)
}

@Serializable
data class Erase(
    @JvmField @SerialName("erase") val tableName: String,
    @JvmField @SerialName("id") val entityId: Any
) : TxOp

@Serializable
data class Sql(
    @JvmField @SerialName("sql") val sql: String,
    @JvmField val argRows: List<List<*>>? = null,
) : TxOp, HasArgs<List<*>, Sql> {

    override fun withArgs(args: List<List<*>>?): Sql = Sql(sql, argRows = args)
}

data class SqlByteArgs(
    @JvmField val sql: String,
    @JvmField val argBytes: ByteBuffer? = null,
) : TxOp

@Serializable
data class XtqlAndArgs(
    @JvmField val op: Xtql,
    @JvmField val args: List<Map<String, *>>? = null
) : TxOp, HasArgs<Map<String, *>, XtqlAndArgs> {

    override fun withArgs(args: List<Map<String, *>>?) = XtqlAndArgs(op, args)
}

@Serializable(Xtql.Serde::class)
sealed interface Xtql: HasArgs<Map<String, *>, XtqlAndArgs> {

    object Serde : JsonContentPolymorphicSerializer<Xtql>(Xtql::class) {
        override fun selectDeserializer(element: JsonElement): DeserializationStrategy<Xtql> = when {
            "insertInto" in element.jsonObject ->  Insert.serializer()
            "updateTable" in element.jsonObject -> Update.serializer()
            "deleteFrom" in element.jsonObject -> Delete.serializer()
            "eraseFrom" in element.jsonObject -> Erase.serializer()
            "assertExists" in element.jsonObject -> AssertExists.serializer()
            "assertNotExists" in element.jsonObject -> AssertNotExists.serializer()
            else -> throw jsonIAE("xtql/malformed-xtql-op", element)
        }
    }

    override fun withArgs(args: List<Map<String, *>>?) = XtqlAndArgs(this, args)

    @Serializable
    data class Insert(
        @JvmField @SerialName("insertInto") val table: String,
        @JvmField val query: Query
    ) : TxOp, Xtql

    @Serializable
    data class Update(
        @JvmField @SerialName("updateTable") val table: String,
        @JvmField val setSpecs: List<Binding>,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>) = copy(unifyClauses = unifyClauses)
    }

    @Serializable
    data class Delete(
        @JvmField @SerialName("deleteFrom") val table: String,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = copy(unifyClauses = unifyClauses)
    }

    @Serializable
    data class Erase(
        @JvmField @SerialName("eraseFrom") val table: String,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = Erase(table, bindSpecs, unifyClauses)
    }

    @Serializable
    data class AssertExists(@JvmField @SerialName("assertExists") val query: Query) : TxOp, Xtql
    @Serializable
    data class AssertNotExists(@JvmField @SerialName("assertNotExists") val query: Query) : TxOp, Xtql

    companion object {
        @JvmStatic
        fun insert(table: String, query: Query): Insert = Insert(table, query)

        @JvmStatic
        fun update(table: String, setSpecs: List<Binding>) = Update(table, setSpecs)

        @JvmStatic
        fun delete(table: String): Delete = Delete(table)

        @JvmStatic
        fun erase(table: String): Erase = Erase(table)

        @JvmStatic
        fun assertExists(query: Query) = AssertExists(query)

        @JvmStatic
        fun assertNotExists(query: Query) = AssertNotExists(query)
    }
}

@Serializable
data class Call(
    @JvmField @SerialName("call") val fnId: Any,
    @JvmField val args: List<Any>
) : TxOp

data object Abort : TxOp
