package xtdb.error

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap
import java.util.*

/**
 * See https://github.com/cognitect-labs/anomalies.
 */
sealed class Anomaly(
    override val message: String?, private val data: IPersistentMap, cause: Throwable?,
) : RuntimeException(message, cause), IExceptionInfo {

    sealed class Caller(message: String?, data: IPersistentMap, cause: Throwable?) : Anomaly(message, data, cause)

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
    }

    override fun getData(): IPersistentMap = data

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is Anomaly) return false

        return message == other.message && data.equiv(other.data) && cause == other.cause
    }

    override fun hashCode() = Objects.hash(message, data, cause)
}