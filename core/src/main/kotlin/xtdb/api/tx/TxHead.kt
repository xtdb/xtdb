package xtdb.api.tx

import xtdb.api.TableRef
import xtdb.api.TransactionKey
import java.time.Instant

/**
 * A partition's latest completed transaction, and where its DML clock stands after it.
 *
 * The ingest clock is [latestCompletedTx]'s ingest time.
 * [systemTime] is the latest system time any transaction took - null until one writes data rows - so it carries
 * forward over transactions that take none.
 */
data class TxHead(val latestCompletedTx: TransactionKey, val systemTime: Instant?) {

    val ingestTime: Instant get() = latestCompletedTx.ingestTime

    fun after(txKey: TransactionKey) = TxHead(txKey, txKey.systemTime ?: systemTime)

    companion object {
        @JvmStatic
        fun after(prev: TxHead?, txKey: TransactionKey) = prev?.after(txKey) ?: TxHead(txKey, txKey.systemTime)
    }
}

/**
 * The tables written and read at ingest time rather than system time: `xt.txs`, which records every transaction,
 * and `xt.role_membership`, which only DDL writes.
 */
object IngestTables {
    private val tables = setOf(TableRef("xt", "txs"), TableRef("xt", "role_membership"))

    @JvmStatic
    fun contains(table: TableRef) = table in tables
}
