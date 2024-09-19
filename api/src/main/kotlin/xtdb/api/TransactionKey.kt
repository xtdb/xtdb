@file:UseSerializers(InstantSerde::class)

package xtdb.api

import kotlinx.serialization.UseSerializers
import xtdb.InstantSerde
import java.time.Instant

/**
 * A key representing a single transaction on the log.
 *
 * @see IXtdb.submitTx
 * @see xtdb.api.query.QueryOptions.basis
 * @see xtdb.api.query.QueryOptions.afterTx
 */
interface TransactionKey : Comparable<TransactionKey> {

    /**
     * the transaction id - a monotonically increasing number.
     */
    val txId: Long

    /**
     * the time as recorded by the transaction log.
     */
    val systemTime: Instant

    /**
     * @suppress
     */
    override fun compareTo(other: TransactionKey) = txId.compareTo(other.txId)
}