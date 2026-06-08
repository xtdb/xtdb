package xtdb

import xtdb.api.log.MessageId
import xtdb.database.DatabaseName
import xtdb.database.encodeTxBasisToken
import xtdb.database.mergeTxBasisTokens

/**
 * A connection's accumulated basis token, providing read-your-writes: [record] folds each committed
 * tx into [value], which the frontend threads into its subsequent reads so the planning snapshot can
 * see this connection's own writes.
 *
 * [value] may also be replaced outright for `SET AWAIT_TOKEN`.
 */
class AwaitToken {
    var value: String? = null

    fun record(dbName: DatabaseName, txId: MessageId) {
        value = mergeTxBasisTokens(value, mapOf(dbName to listOf(txId)).encodeTxBasisToken())
    }
}
