package xtdb

import xtdb.api.Xtdb
import xtdb.api.log.MessageId
import xtdb.database.DatabaseName
import xtdb.database.encodeTxBasisToken
import xtdb.database.mergeTxBasisTokens
import xtdb.indexer.Snapshot
import xtdb.query.PreparedQuery
import xtdb.tx.TxOp
import xtdb.tx.TxOpts

class Session(private val node: Node, var dbName: DatabaseName) : AutoCloseable {

    interface Node {
        fun submitTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.SubmittedTx
        fun executeTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.ExecutedTx
        fun openSqlQuery(sql: String, dbName: DatabaseName, awaitToken: String?): ResultCursor
        fun prepareSql(sql: String, dbName: DatabaseName, awaitToken: String?): PreparedQuery
        fun openSnapshot(dbName: DatabaseName, awaitToken: String?): Snapshot
    }

    var awaitToken: String? = null

    fun recordTx(dbName: DatabaseName, txId: MessageId) {
        awaitToken = mergeTxBasisTokens(awaitToken, mapOf(dbName to listOf(txId)).encodeTxBasisToken())
    }

    fun submitTx(ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.SubmittedTx =
        node.submitTx(dbName, ops, opts).also { recordTx(dbName, it.txId) }

    fun executeTx(ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.ExecutedTx =
        node.executeTx(dbName, ops, opts).also { recordTx(dbName, it.txId) }

    fun openSqlQuery(sql: String): ResultCursor = node.openSqlQuery(sql, dbName, awaitToken)
    fun prepareSql(sql: String): PreparedQuery = node.prepareSql(sql, dbName, awaitToken)
    fun openSnapshot(): Snapshot = node.openSnapshot(dbName, awaitToken)

    override fun close() {}
}
