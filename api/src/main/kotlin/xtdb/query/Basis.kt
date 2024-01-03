package xtdb.query

import clojure.lang.*
import xtdb.api.TransactionKey
import java.time.Instant
import java.util.ArrayList

private val AT_TX_KEY: Keyword = Keyword.intern("at-tx")
private val CURRENT_TIME_KEY: Keyword = Keyword.intern("current-time")

data class Basis(
    @JvmField val atTx: TransactionKey?,
    @JvmField val currentTime: Instant?
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

    override fun seq(): ISeq {
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
