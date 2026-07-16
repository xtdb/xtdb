package xtdb.api.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Busy(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly(message, ensureCategory(data, BUSY), cause) {
    internal companion object {
        val BUSY: Keyword = Keyword.intern("cognitect.anomalies", "busy")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Busy: $errorCode", dataFromMap(BUSY, errorCode, data), cause)

    override fun mergeCtx(ctx: Map<String, *>): Busy =
        Busy(message, mergeCtx(getData(), ctx), cause)
}