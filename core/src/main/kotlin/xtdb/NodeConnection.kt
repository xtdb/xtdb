package xtdb

import xtdb.api.Xtdb
import xtdb.api.log.MessageId
import xtdb.database.DatabaseName
import xtdb.database.encodeTxBasisToken
import xtdb.database.mergeTxBasisTokens
import xtdb.indexer.DatabaseSnapshot
import xtdb.query.PreparedQuery
import xtdb.tx.TxOp
import xtdb.tx.TxOpts

/**
 * A connection-scoped handle onto the node, shared by the protocol frontends (ADBC's [XtdbConnection],
 * pgwire) so that read-your-writes is maintained in one place rather than re-derived at every callsite.
 *
 * Each write advances [awaitToken]; each read threads it through, so the planning snapshot can see this
 * connection's own writes. Because writes and reads go through here, no callsite can forget to do either.
 *
 * [awaitToken] is exposed only for the pgwire `SET`/`SHOW AWAIT_TOKEN` SQL feature.
 */
class NodeConnection(private val node: Node, var dbName: DatabaseName) {

    interface Node {
        fun submitTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.SubmittedTx
        fun executeTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.ExecutedTx
        fun openSqlQuery(sql: String, dbName: DatabaseName, awaitToken: String?): ResultCursor
        fun prepareSql(sql: String, dbName: DatabaseName, awaitToken: String?): PreparedQuery
        fun openSnapshot(dbName: DatabaseName, awaitToken: String?): DatabaseSnapshot
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
    fun openSnapshot(): DatabaseSnapshot = node.openSnapshot(dbName, awaitToken)
}
