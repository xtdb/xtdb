package xtdb.api.query

import clojure.lang.*
import xtdb.api.TransactionKey
import java.time.Instant

private val AT_TX_KEY: Keyword = Keyword.intern("at-tx")
private val CURRENT_TIME_KEY: Keyword = Keyword.intern("current-time")

/**
 * XTDB queries are subject to a 'basis' - queries run again with the same basis are guaranteed to return the same results.
 *
 * @see atTx
 * @see currentTime
 */
data class Basis(
    /**
     * An upper bound on the transactions visible to the query - the query will not see the effects of any transaction after this one.
     *
     * If not provided, this will default to the latest completed transaction on the node executing the query.
     */
    @JvmField val atTx: TransactionKey? = null,

    /**
     * Explicitly specifies the 'current time' of the query, for any functions that rely on the wall-clock time
     * (e.g. `current_timestamp`, but also the default valid-time at which tables are read).
     *
     * If not provided, this defaults to the actual wall-clock time on the node executing the query.
     */
    @JvmField val currentTime: Instant? = null
) : ILookup, Seqable {

    /**
     * @suppress
     */
    override fun valAt(key: Any?): Any? {
        return valAt(key, null)
    }

    /**
     * @suppress
     */
    override fun valAt(key: Any?, notFound: Any?) =
        when {
            key === AT_TX_KEY -> atTx
            key === CURRENT_TIME_KEY -> currentTime
            else -> notFound
        }

    /**
     * @suppress
     */
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
