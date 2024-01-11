package xtdb

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

private val ERROR_KEY: Keyword = Keyword.intern("xtdb.error", "error-key")

@Suppress("unused")
data class RuntimeException(
    val key: Keyword?,
    override val message: String? = "Runtime error: '${key?.sym}'",
    private val data: Map<*, *> = emptyMap<Keyword, Any>(),
    override val cause: Throwable? = null,
) : java.lang.RuntimeException(message, cause), IExceptionInfo {

    override fun getData(): IPersistentMap =
        (data as? IPersistentMap ?: PersistentHashMap.create(data)).assoc(ERROR_KEY, key)

}
