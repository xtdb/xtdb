package xtdb.api.error

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap
import com.fasterxml.jackson.core.JsonParseException
import java.nio.channels.ClosedByInterruptException
import java.sql.BatchUpdateException
import java.time.format.DateTimeParseException
import java.util.*

/**
 * See https://github.com/cognitect-labs/anomalies.
 */
sealed class Anomaly(
    override val message: String?, private val data: IPersistentMap, cause: Throwable?,
) : RuntimeException(message, cause), IExceptionInfo {

    sealed class Caller(message: String?, data: IPersistentMap, cause: Throwable?) : Anomaly(message, data, cause)

    /**
     * Return a fresh anomaly of the same category with [ctx] merged into the existing data,
     * preserving this instance as the cause. An empty [ctx] returns this instance unchanged.
     */
    abstract fun mergeCtx(ctx: Map<String, *>): Anomaly

    companion object {
        internal val CATEGORY = Keyword.intern("cognitect.anomalies", "category")
        internal val ERROR_CODE = Keyword.intern("xtdb.error", "code")

        internal fun ensureCategory(data: IPersistentMap, category: Keyword): IPersistentMap {
            return if (data.containsKey(CATEGORY)) data else data.assoc(CATEGORY, category)
        }

        internal fun dataFromMap(category: Keyword, errorCode: String?, data: Map<String, *>?) =
            PersistentHashMap.create(data.orEmpty().mapKeys { (k, _) -> Keyword.intern(k) })
                .assoc(CATEGORY, category)
                .let {
                    var data = it
                    if (errorCode != null) data = data.assoc(ERROR_CODE, Keyword.intern(errorCode))
                    data
                }

        /**
         * Merge [r]'s entries (string-keyed; each key gets interned to a [Keyword]) into [l],
         * returning a new persistent map. Used by [Anomaly.mergeCtx] implementations.
         */
        @JvmStatic
        fun mergeCtx(l: IPersistentMap, r: Map<String, *>): IPersistentMap =
            r.entries.fold(l) { acc, (k, v) -> acc.assoc(Keyword.intern(k), v) }

        /**
         * Convert a [Throwable] to the appropriate [Anomaly] subclass, attaching [ctx] as
         * extra context data. An incoming [Anomaly] is wrapped in a fresh instance of the
         * same category with the merged context, preserving the original as the cause.
         *
         * Mirrors `xtdb.error/->anomaly`; keep them in sync until the remaining Clojure
         * callers move across.
         */
        @JvmStatic
        @JvmOverloads
        fun Throwable.toAnomaly(ctx: Map<String, *> = emptyMap<String, Any?>()): Anomaly = when (this) {
            is Anomaly -> mergeCtx(ctx)
            is NumberFormatException -> Incorrect(message, "xtdb.error/number-format", ctx, this)
            is IllegalArgumentException -> Incorrect(message, "xtdb.error/illegal-arg", ctx, this)
            is DateTimeParseException -> Incorrect(
                message, "xtdb.error/date-time-parse",
                ctx + ("date-time" to parsedString),
                this,
            )
            is ClosedByInterruptException ->
                Interrupted(message ?: "Interrupted", "xtdb.error/interrupted", ctx, this)
            is InterruptedException -> Interrupted(message, "xtdb.error/interrupted", ctx, this)
            is UnsupportedOperationException -> Unsupported(message, "xtdb.error/unsupported", ctx, this)
            is JsonParseException -> Incorrect(message, "xtdb.error/json-parse", ctx, this)
            is BatchUpdateException ->
                cause?.toAnomaly(ctx) ?: Fault(message, "xtdb.error/unknown", ctx, this)
            else -> Fault(message, "xtdb.error/unknown", ctx, this)
        }

        /**
         * Run [block], catching any [Throwable] and rethrowing as the appropriate [Anomaly]
         * with [ctx] merged into its data. Mirrors `xtdb.error/wrap-anomaly`.
         */
        inline fun <R> wrapAnomaly(ctx: Map<String, *> = emptyMap<String, Any?>(), block: () -> R): R = try {
            block()
        } catch (e: Throwable) {
            throw e.toAnomaly(ctx)
        }
    }

    override fun getData(): IPersistentMap = data

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is Anomaly) return false

        return message == other.message && data.equiv(other.data) && cause == other.cause
    }

    override fun hashCode() = Objects.hash(message, data, cause)
}