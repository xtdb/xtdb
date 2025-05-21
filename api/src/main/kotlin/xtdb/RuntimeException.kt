package xtdb

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap
import clojure.lang.Util

private val ERROR_KEY: Keyword = Keyword.intern("xtdb.error", "error-key")

/**
 * @suppress
 */
@Suppress("unused")
data class RuntimeException(
    val key: Keyword?,
    override val message: String? = "Runtime error: '${key?.sym}'",
    private val data: Map<*, *> = emptyMap<Keyword, Any>(),
    override val cause: Throwable? = null,
) : java.lang.RuntimeException(message, cause), IExceptionInfo {

    override fun getData(): IPersistentMap =
        (data as? IPersistentMap ?: PersistentHashMap.create(data)).assoc(ERROR_KEY, key)

    // exclude `cause`
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RuntimeException) return false

        if (key != other.key) return false
        if (message != other.message) return false
        if (!Util.equiv(data, other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key?.hashCode() ?: 0
        result = 31 * result + (message?.hashCode() ?: 0)
        result = 31 * result + Util.hash(data)
        return result
    }
}
