package xtdb

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

private val ERROR_KEY: Keyword = Keyword.intern("xtdb.error", "error-key")

@Suppress("unused")
data class IllegalArgumentException(
    val key: Keyword?,
    override val message: String? = "Illegal argument: '${key?.sym}'",
    private val data: Map<*, *> = emptyMap<Keyword, Any>(),
    override val cause: Throwable? = null,
) : java.lang.IllegalArgumentException(message, cause), IExceptionInfo {

    override fun getData(): IPersistentMap =
        (data as? IPersistentMap ?: PersistentHashMap.create(data)).assoc(ERROR_KEY, key)

    companion object {
        @JvmStatic
        @JvmOverloads
        fun createNoKey(message: String, data: Map<*, *>, cause: Throwable? = null) =
            IllegalArgumentException(null, message, data, cause)

        @JvmStatic
        @JvmOverloads
        fun create(key: Keyword?, data: Map<*, *>, cause: Throwable? = null) =
            IllegalArgumentException(key, data = data, cause = cause)
    }
}
