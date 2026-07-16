package xtdb.api.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Unavailable(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly(message, ensureCategory(data, UNAVAILABLE), cause) {

    internal companion object {
        val UNAVAILABLE: Keyword = Keyword.intern("cognitect.anomalies", "unavailable")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Unavailable: $errorCode", dataFromMap(UNAVAILABLE, errorCode, data), cause)

    override fun mergeCtx(ctx: Map<String, *>): Unavailable =
        Unavailable(message, mergeCtx(getData(), ctx), cause)
}