@file:UseSerializers(InstantSerde::class)
package xtdb.api

import clojure.lang.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.InstantSerde
import java.time.Instant

private val TX_ID_KEY: Keyword = Keyword.intern("tx-id")
private val SYSTEM_TIME_KEY: Keyword = Keyword.intern("system-time")

@Serializable
data class TransactionKey(val txId: Long, val systemTime: Instant) : Comparable<TransactionKey>, ILookup, Seqable {
    override fun compareTo(other: TransactionKey) = txId.compareTo(other.txId)

    fun withSystemTime(systemTime: Instant) = TransactionKey(this.txId, systemTime)

    override fun valAt(key: Any?) = valAt(key, null)

    override fun valAt(key: Any?, notFound: Any?) =
        when {
            key === TX_ID_KEY -> txId
            key === SYSTEM_TIME_KEY -> systemTime
            else -> notFound
        }

    override fun seq(): ISeq? =
        PersistentList.create(
            listOf(MapEntry.create(TX_ID_KEY, txId), MapEntry.create(SYSTEM_TIME_KEY, systemTime))
        ).seq()
}
