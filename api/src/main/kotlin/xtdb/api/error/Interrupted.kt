package xtdb.api.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Interrupted(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly(message, ensureCategory(data, INTERRUPTED), cause){

    internal companion object {
        val INTERRUPTED: Keyword = Keyword.intern("cognitect.anomalies", "interrupted")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Interrupted", dataFromMap(INTERRUPTED, errorCode, data), cause)

    override fun mergeCtx(ctx: Map<String, *>): Interrupted =
        Interrupted(message, mergeCtx(getData(), ctx), cause)
}