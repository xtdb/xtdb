package xtdb.api.tx

import xtdb.api.query.Binding
import xtdb.api.query.Query
import xtdb.api.query.Query.UnifyClause
import xtdb.api.query.TemporalFilter.TemporalExtents
import xtdb.api.tx.TxOp.HasArgs
import xtdb.api.tx.TxOp.HasValidTimeBounds
import xtdb.types.ClojureForm
import java.nio.ByteBuffer
import java.time.Instant

private const val XT_TXS = "xt/tx_fns"
private const val XT_ID = "xt/id"
private const val XT_FN = "xt/fn"

sealed interface TxOp {
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
        fun put(tableName: String, doc: Map<String, *>): Put = Put(tableName, doc)

        @JvmStatic
        fun putFn(fnId: Any, fnForm: Any): Put = put(XT_TXS, mapOf(XT_ID to fnId, XT_FN to ClojureForm(fnForm)))

        @JvmStatic
        fun delete(tableName: String, entityId: Any): Delete = Delete(tableName, entityId)

        @JvmStatic
        fun erase(tableName: String, entityId: Any): Erase = Erase(tableName, entityId)

        @JvmStatic
        fun call(fnId: Any, args: List<*>): Call = Call(fnId, args)

        @JvmField
        val ABORT = Abort
    }
}

data class Put(
    @JvmField val tableName: String,
    @JvmField val doc: Map<String, *>,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null
) : TxOp, HasValidTimeBounds<Put> {

    override fun startingFrom(validFrom: Instant?): Put = Put(tableName, doc, validFrom, validTo)
    override fun until(validTo: Instant?): Put = Put(tableName, doc, validFrom, validTo)
    override fun during(validFrom: Instant?, validTo: Instant?): Put = Put(tableName, doc, validFrom, validTo)
}

data class Delete(
    @JvmField val tableName: String,
    @JvmField val entityId: Any,
    @JvmField val validFrom: Instant? = null,
    @JvmField val validTo: Instant? = null
) : TxOp, HasValidTimeBounds<Delete> {

    override fun startingFrom(validFrom: Instant?): Delete = Delete(tableName, entityId, validFrom, validTo)
    override fun until(validTo: Instant?): Delete = Delete(tableName, entityId, validFrom, validTo)
    override fun during(validFrom: Instant?, validTo: Instant?) = Delete(tableName, entityId, validFrom, validTo)
}

data class Erase(
    @JvmField val tableName: String,
    @JvmField val entityId: Any
) : TxOp

data class Sql(
    @JvmField val sql: String,
    @JvmField val argRows: List<List<*>>? = null,
    @JvmField val argBytes: ByteBuffer? = null
) : TxOp, HasArgs<List<*>, Sql> {

    override fun withArgs(args: List<List<*>>?): Sql = Sql(sql, argRows = args)
}

data class XtqlAndArgs(
    @JvmField val op: Xtql,
    @JvmField val args: List<Map<*, *>>? = null
) : TxOp, HasArgs<Map<*, *>, XtqlAndArgs> {

    override fun withArgs(args: List<Map<*, *>>?) = XtqlAndArgs(op, args)
}

sealed interface Xtql: HasArgs<Map<*, *>, XtqlAndArgs> {
    override fun withArgs(args: List<Map<*, *>>?) = XtqlAndArgs(this, args)

    data class Insert(
        @JvmField val table: String,
        @JvmField val query: Query
    ) : TxOp, Xtql

    data class Update(
        @JvmField val table: String,
        @JvmField val setSpecs: List<Binding>,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>) = copy(unifyClauses = unifyClauses)
    }

    data class Delete(
        @JvmField val table: String,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = copy(unifyClauses = unifyClauses)
    }

    data class Erase(
        @JvmField val table: String,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null
    ) : TxOp, Xtql {

        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = Erase(table, bindSpecs, unifyClauses)
    }

    data class AssertExists(@JvmField val query: Query) : TxOp, Xtql
    data class AssertNotExists(@JvmField val query: Query) : TxOp, Xtql

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

data class Call(
    @JvmField val fnId: Any,
    @JvmField val args: List<*>
) : TxOp

data object Abort : TxOp
