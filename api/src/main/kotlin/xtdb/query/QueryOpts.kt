package xtdb.query

import clojure.lang.*
import xtdb.api.TransactionKey
import java.time.Duration
import java.time.ZoneId
import java.util.ArrayList

private val ARGS_KEY: Keyword = Keyword.intern("args")
private val BASIS_KEY: Keyword = Keyword.intern("basis")
private val AFTER_TX_KEY: Keyword = Keyword.intern("after-tx")
private val TX_TIMEOUT_KEY: Keyword = Keyword.intern("tx-timeout")
private val DEFAULT_TZ_KEY: Keyword = Keyword.intern("default-tz")
private val EXPLAIN_KEY: Keyword? = Keyword.intern("explain?")
private val KEY_FN_KEY: Keyword = Keyword.intern("key-fn")

data class QueryOpts(
    val args: Map<String, Any>? = null,
    val basis: Basis? = null,
    val afterTx: TransactionKey? = null,
    val txTimeout: Duration? = null,
    val defaultTz: ZoneId? = null,
    val explain: Boolean = false,
    val keyFn: String? = "snake_case"
) : ILookup, Seqable {
    fun args() = args
    fun basis() = basis
    fun afterTx() = afterTx
    fun txTimeout() = txTimeout
    fun defaultTz() = defaultTz
    fun explain() = explain
    fun keyFn() = keyFn

    override fun valAt(key: Any?): Any? {
        return valAt(key, null)
    }

    override fun valAt(key: Any?, notFound: Any?): Any? {
        return when {
            key === ARGS_KEY -> args
            key === BASIS_KEY -> basis
            key === AFTER_TX_KEY -> afterTx
            key === TX_TIMEOUT_KEY -> txTimeout
            key === DEFAULT_TZ_KEY -> defaultTz
            key === EXPLAIN_KEY -> explain
            key === KEY_FN_KEY -> keyFn
            else -> notFound
        }
    }

    override fun seq(): ISeq? {
        val seqList: MutableList<Any?> = ArrayList()
        seqList.add(MapEntry.create(ARGS_KEY, args))

        if (basis != null) {
            seqList.add(MapEntry.create(BASIS_KEY, basis))
        }
        if (afterTx != null) {
            seqList.add(MapEntry.create(AFTER_TX_KEY, afterTx))
        }
        if (txTimeout != null) {
            seqList.add(MapEntry.create(TX_TIMEOUT_KEY, txTimeout))
        }
        if (defaultTz != null) {
            seqList.add(MapEntry.create(DEFAULT_TZ_KEY, defaultTz))
        }
        if (EXPLAIN_KEY != null) {
            seqList.add(MapEntry.create(EXPLAIN_KEY, explain))
        }

        seqList.add(MapEntry.create(KEY_FN_KEY, keyFn))

        return PersistentList.create(seqList).seq()
    }
}
