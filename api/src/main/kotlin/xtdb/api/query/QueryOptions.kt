@file:UseSerializers(AnySerde::class, InstantSerde::class, DurationSerde::class, ZoneIdSerde::class)

package xtdb.api.query

import clojure.lang.*
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import xtdb.*
import xtdb.AnySerde.toValue
import xtdb.api.TransactionKey
import java.time.Duration
import java.time.ZoneId

private val ARGS_KEY: Keyword = Keyword.intern("args")
private val BASIS_KEY: Keyword = Keyword.intern("basis")
private val AFTER_TX_KEY: Keyword = Keyword.intern("after-tx")
private val TX_TIMEOUT_KEY: Keyword = Keyword.intern("tx-timeout")
private val DEFAULT_TZ_KEY: Keyword = Keyword.intern("default-tz")
private val EXPLAIN_KEY: Keyword? = Keyword.intern("explain?")
private val KEY_FN_KEY: Keyword = Keyword.intern("key-fn")

@Serializable
data class QueryOptions(
    @JvmField @Serializable(ArgsSerde::class) val args: Map<String, *>? = null,
    @JvmField val basis: Basis? = null,
    @JvmField val afterTx: TransactionKey? = null,
    @JvmField val txTimeout: Duration? = null,
    @JvmField val defaultTz: ZoneId? = null,
    @JvmField val explain: Boolean = false,
    @JvmField val keyFn: IKeyFn<*>? = null
) : ILookup, Seqable {

    internal object ArgsSerde: KSerializer<Map<String, *>> {
        @OptIn(ExperimentalSerializationApi::class)
        override val descriptor = SerialDescriptor("xtdb.ArgsSerde", JsonElement.serializer().descriptor)

        override fun serialize(encoder: Encoder, value: Map<String, *>) = AnySerde.serialize(encoder, value)

        @Suppress("UNCHECKED_CAST")
        override fun deserialize(decoder: Decoder): Map<String, *> {
            require(decoder is JsonDecoder)
            return when (val element = decoder.decodeJsonElement()) {
                is JsonArray -> element.map { it.toValue() } .mapIndexed{ idx, arg -> "_$idx" to arg }.toMap()
                is JsonObject -> element.toValue()
                else -> throw jsonIAE("unknown-args-json-type", element)
            } as Map<String, *>
        }
    }

    /**
     * @suppress
     */
    companion object {
        @JvmStatic
        fun queryOpts() = Builder()
    }

    /**
     * @suppress
     */
    override fun valAt(key: Any?): Any? {
        return valAt(key, null)
    }

    /**
     * @suppress
     */
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

    /**
     * @suppress
     */
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

        keyFn?.let { seqList.add(MapEntry.create(KEY_FN_KEY, keyFn)) }

        seqList.add(MapEntry.create(EXPLAIN_KEY, explain))

        return PersistentList.create(seqList).seq()
    }

    class Builder internal constructor() {
        private var args: Map<String, Any>? = null
        private var basis: Basis? = null
        private var afterTx: TransactionKey? = null
        private var txTimeout: Duration? = null
        private var defaultTz: ZoneId? = null
        private var explain: Boolean = false
        private var keyFn: IKeyFn<Any>? = null

        /**
         * Supply query arguments as a map (e.g. for named parameters in an XTQL query)
         */
        fun args(args: Map<String, Any>?) = apply { this.args = args }

        /**
         * Supply query arguments as a list (e.g. for positional parameters in an SQL query)
         */
        fun args(args: List<Any>?) = apply { this.args = args?.mapIndexed { idx, arg -> "_$idx" to arg }?.toMap() }

        /**
         * The basis of the query - queries with the same (fully-specified) basis are guaranteed to return the same
         * results, no matter when/where they're run.
         *
         * @see Basis
         */
        fun basis(basis: Basis?) = apply { this.basis = basis }

        /**
         * The lower-bound transaction of the query - the query will wait until _at least_ this transaction has been indexed before running the query.
         *
         * If not provided, this defaults to the latest transaction submitted through this client - this means that (assuming you query the same client that you submitted to) you will always see the effect of any transaction you've submitted (known as 'reading your writes').
         *
         * (If you're not querying through the same client you submitted to, pass your result from submitTx as [afterTx] to achieve the same effect.)
         */
        fun afterTx(afterTx: TransactionKey?) = apply { this.afterTx = afterTx }

        /**
         * Time to wait for the requested transaction (either [Basis.atTx] or [afterTx], whichever is later) to be indexed by the executing node.
         *
         * If this timeout is exceeded, the query will throw a [java.util.concurrent.TimeoutException].
         *
         * If not provided, the query will wait indefinitely.
         */
        fun txTimeout(txTimeout: Duration?) = apply { this.txTimeout = txTimeout }

        /**
         * The default time-zone that applies to any functions within the query without an explicitly specified time-zone.
         *
         * If not provided, defaults to UTC.
         */
        fun defaultTz(defaultTz: ZoneId?) = apply { this.defaultTz = defaultTz }

        /**
         * If set, the query will return its 'explain plan' (a plan of the operations it would perform, similar to SQL `EXPLAIN`) rather than the query results.
         */
        fun explain(explain: Boolean) = apply { this.explain = explain }

        /**
         * Specifies the casing of object keys in the query results.
         *
         * If not provided, will default to [IKeyFn.KeyFn.CAMEL_CASE_STRING] - e.g. `firstName`, `_id`, `_validTime`.
         *
         * @see IKeyFn
         * @see IKeyFn.KeyFn
         */
        fun keyFn(keyFn: IKeyFn<Any>?) = apply { this.keyFn = keyFn }

        /**
         * build the [QueryOptions] object.
         */
        fun build() = QueryOptions(args, basis, afterTx, txTimeout, defaultTz, explain, keyFn)
    }
}

fun queryOpts() = QueryOptions.queryOpts()
