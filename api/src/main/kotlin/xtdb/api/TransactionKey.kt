@file:UseSerializers(InstantSerde::class)

package xtdb.api

import clojure.lang.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.InstantSerde
import java.time.Instant

private val TX_ID_KEY: Keyword = Keyword.intern("tx-id")
private val SYSTEM_TIME_KEY: Keyword = Keyword.intern("system-time")

/**
 * A key representing a single transaction on the log.
 *
 * @see IXtdb.submitTx
 * @see xtdb.api.query.QueryOptions.basis
 * @see xtdb.api.query.QueryOptions.afterTx
 */
@Serializable
data class TransactionKey(
    /**
     * the transaction id - a monotonically increasing number.
     */
    val txId: Long,
    /**
     * the time as recorded by the transaction log.
     */
    val systemTime: Instant,
) : Comparable<TransactionKey>, ILookup, Seqable {
    /**
     * @suppress
     */
    override fun compareTo(other: TransactionKey) = txId.compareTo(other.txId)

    /**
     * @suppress
     */
    @Suppress("unused")
    fun withSystemTime(systemTime: Instant) = TransactionKey(this.txId, systemTime)

    /**
     * @suppress
     */
    override fun valAt(key: Any?) = valAt(key, null)

    /**
     * @suppress
     */
    override fun valAt(key: Any?, notFound: Any?) = when {
        key === TX_ID_KEY -> txId
        key === SYSTEM_TIME_KEY -> systemTime
        else -> notFound
    }

    /**
     * @suppress
     */
    override fun seq(): ISeq? = PersistentList.create(
        listOf(MapEntry.create(TX_ID_KEY, txId), MapEntry.create(SYSTEM_TIME_KEY, systemTime))
    ).seq()
}
