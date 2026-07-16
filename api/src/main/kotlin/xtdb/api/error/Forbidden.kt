package xtdb.api.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Forbidden(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly.Caller(message, ensureCategory(data, FORBIDDEN), cause) {
    internal companion object {
        val FORBIDDEN: Keyword = Keyword.intern("cognitect.anomalies", "forbidden")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Forbidden: $errorCode", dataFromMap(FORBIDDEN, errorCode, data), cause)

    override fun mergeCtx(ctx: Map<String, *>): Forbidden =
        Forbidden(message, mergeCtx(getData(), ctx), cause)
}