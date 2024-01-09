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
    @JvmField val args: Map<String, *>? = null,
    @JvmField val basis: Basis? = null,
    @JvmField val afterTx: TransactionKey? = null,
    @JvmField val txTimeout: Duration? = null,
    @JvmField val defaultTz: ZoneId? = null,
    @JvmField val explain: Boolean = false,
    @JvmField val keyFn: String? = null
) : ILookup, Seqable {

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

        basis?.let { seqList.add(MapEntry.create(BASIS_KEY, it)) }

        if (afterTx != null) {
            seqList.add(MapEntry.create(AFTER_TX_KEY, afterTx))
        }
        if (txTimeout != null) {
            seqList.add(MapEntry.create(TX_TIMEOUT_KEY, txTimeout))
        }
        if (defaultTz != null) {
            seqList.add(MapEntry.create(DEFAULT_TZ_KEY, defaultTz))
        }

        seqList.add(MapEntry.create(EXPLAIN_KEY, explain))
        seqList.add(MapEntry.create(KEY_FN_KEY, keyFn))

        return PersistentList.create(seqList).seq()
    }

    companion object {
        @JvmStatic
        fun queryOpts() = Builder()
    }

    class Builder {
        private var args: Map<String, *>? = null
        private var basis: Basis? = null
        private var afterTx: TransactionKey? = null
        private var txTimeout: Duration? = null
        private var defaultTz: ZoneId? = null
        private var explain: Boolean = false
        private var keyFn: String? = null

        fun args(args: Map<String, *>?) = apply { this.args = args }
        fun basis(basis: Basis?) = apply { this.basis = basis }
        fun afterTx(afterTx: TransactionKey?) = apply { this.afterTx = afterTx }
        fun txTimeout(txTimeout: Duration?) = apply { this.txTimeout = txTimeout }
        fun defaultTz(defaultTz: ZoneId?) = apply { this.defaultTz = defaultTz }
        fun explain(explain: Boolean) = apply { this.explain = explain }
        fun keyFn(keyFn: String?) = apply { this.keyFn = keyFn }

        fun build() = QueryOpts(args, basis, afterTx, txTimeout, defaultTz, explain, keyFn)
    }
}
