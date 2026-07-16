package xtdb.api.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class NotFound(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly.Caller(message, ensureCategory(data, NOT_FOUND), cause) {

    internal companion object {
        val NOT_FOUND: Keyword = Keyword.intern("cognitect.anomalies", "not-found")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Not found: $errorCode", dataFromMap(NOT_FOUND, errorCode, data), cause)

    override fun mergeCtx(ctx: Map<String, *>): NotFound =
        NotFound(message, mergeCtx(getData(), ctx), cause)
}