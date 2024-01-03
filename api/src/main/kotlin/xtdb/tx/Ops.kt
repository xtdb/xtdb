package xtdb.tx

import clojure.lang.Keyword
import xtdb.tx.Ops.HasArgs
import xtdb.tx.Ops.HasValidTimeBounds
import xtdb.types.ClojureForm
import java.nio.ByteBuffer
import java.time.Instant

private val XT_TXS: Keyword = Keyword.intern("xt", "tx-fns")
private val XT_ID: Keyword = Keyword.intern("xt", "id")
private val XT_FN: Keyword = Keyword.intern("xt", "fn")

sealed class Ops {
    @Suppress("unused")
    interface HasArgs<ArgType, O : HasArgs<ArgType, O>?> {
        fun withArgs(args: List<ArgType>?): O

        fun withArgs(vararg args: ArgType): O {
            return withArgs(listOf(*args))
        }
    }

    interface HasValidTimeBounds<O : HasValidTimeBounds<O>?> {
        fun startingFrom(validFrom: Instant?): O
        fun until(validTo: Instant?): O
        fun during(validFrom: Instant?, validTo: Instant?): O
    }

    companion object {
        @JvmStatic
        fun sql(sql: String): Sql = Sql(sql)

        @JvmStatic
        fun sql(sql: String, paramRow: List<*>): Sql = Sql(sql, listOf(paramRow))

        @JvmStatic
        fun sqlBatch(sql: String, paramGroupRows: List<List<*>>?): Sql = Sql(sql, argRows = paramGroupRows)

        @JvmStatic
        fun sqlBatch(sql: String, paramGroupBytes: ByteBuffer?): Sql = Sql(sql, argBytes = paramGroupBytes)

        @JvmStatic
        fun sqlBatch(sql: String, paramGroupBytes: ByteArray?): Sql = sqlBatch(sql, ByteBuffer.wrap(paramGroupBytes))

        @JvmStatic
        fun xtql(query: Any): Xtql = Xtql(query)

        @JvmStatic
        fun put(tableName: Keyword, doc: Map<*, *>): Put = Put(tableName, doc)

        @JvmStatic
        fun putFn(fnId: Any, fnForm: Any): Put = put(XT_TXS, mapOf(XT_ID to fnId, XT_FN to ClojureForm(fnForm)))

        @JvmStatic
        fun delete(tableName: Keyword, entityId: Any): Delete = Delete(tableName, entityId)

        @JvmStatic
        fun erase(tableName: Keyword, entityId: Any): Erase = Erase(tableName, entityId)

        @JvmStatic
        fun call(fnId: Any, args: List<*>): Call = Call(fnId, args)

        @JvmField
        val ABORT = Abort
    }
}

data class Put internal constructor(
    @JvmField val tableName: Keyword,
    @JvmField val doc: Map<*, *>,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null
) : Ops(), HasValidTimeBounds<Put> {

    override fun startingFrom(validFrom: Instant?): Put = Put(tableName, doc, validFrom, validTo)
    override fun until(validTo: Instant?): Put = Put(tableName, doc, validFrom, validTo)
    override fun during(validFrom: Instant?, validTo: Instant?): Put = Put(tableName, doc, validFrom, validTo)
}

data class Delete internal constructor(
    @JvmField val tableName: Keyword,
    @JvmField val entityId: Any,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null
) : Ops(), HasValidTimeBounds<Delete> {

    override fun startingFrom(validFrom: Instant?): Delete = Delete(tableName, entityId, validFrom, validTo)
    override fun until(validTo: Instant?): Delete = Delete(tableName, entityId, validFrom, validTo)
    override fun during(validFrom: Instant?, validTo: Instant?) = Delete(tableName, entityId, validFrom, validTo)
}

data class Erase(
    @JvmField val tableName: Keyword,
    @JvmField val entityId: Any
) : Ops()

data class Sql(
    @JvmField val sql: String,
    @JvmField val argRows: List<List<*>>? = null,
    @JvmField val argBytes: ByteBuffer? = null
) : Ops(), HasArgs<List<*>, Sql> {

    override fun withArgs(args: List<List<*>>?): Sql = Sql(sql, argRows = args)
}

data class Xtql internal constructor(
    @JvmField val query: Any,
    @JvmField val args: List<Map<*, *>>? = null
) : Ops(), HasArgs<Map<*, *>, Xtql> {

    override fun withArgs(args: List<Map<*, *>>?) = Xtql(query, args)
}

data class Call internal constructor(
    @JvmField val fnId: Any,
    @JvmField val args: List<*>
) : Ops()

data object Abort : Ops()
