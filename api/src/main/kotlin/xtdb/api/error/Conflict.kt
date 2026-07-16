package xtdb.api.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Conflict(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly.Caller(message, ensureCategory(data, CONFLICT), cause) {

    internal companion object {
        val CONFLICT: Keyword = Keyword.intern("cognitect.anomalies", "conflict")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Conflict: $errorCode", dataFromMap(CONFLICT, errorCode, data), cause)

    override fun mergeCtx(ctx: Map<String, *>): Conflict =
        Conflict(message, mergeCtx(getData(), ctx), cause)
}