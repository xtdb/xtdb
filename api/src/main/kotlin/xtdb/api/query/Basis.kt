@file:UseSerializers(InstantSerde::class)
package xtdb.api.query

import clojure.lang.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.InstantSerde
import xtdb.api.TransactionKey
import java.time.Instant

private val AT_TX_KEY: Keyword = Keyword.intern("at-tx")
private val CURRENT_TIME_KEY: Keyword = Keyword.intern("current-time")

@Serializable
data class Basis(
    @JvmField val atTx: TransactionKey? = null,
    @JvmField val currentTime: Instant? = null
) : ILookup, Seqable {

    override fun valAt(key: Any?): Any? {
        return valAt(key, null)
    }

    override fun valAt(key: Any?, notFound: Any?) =
        when {
            key === AT_TX_KEY -> atTx
            key === CURRENT_TIME_KEY -> currentTime
            else -> notFound
        }

    override fun seq(): ISeq? {
        val seqList: MutableList<Any?> = ArrayList()
        if (atTx != null) {
            seqList.add(MapEntry.create(AT_TX_KEY, atTx))
        }
        if (currentTime != null) {
            seqList.add(MapEntry.create(CURRENT_TIME_KEY, currentTime))
        }
        return PersistentList.create(seqList).seq()
    }
}
