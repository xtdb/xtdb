package xtdb.api.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Fault(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly(message, ensureCategory(data, FAULT), cause) {
    internal companion object {
        val FAULT: Keyword = Keyword.intern("cognitect.anomalies", "fault")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Fault: $errorCode", dataFromMap(FAULT, errorCode, data), cause)

    override fun mergeCtx(ctx: Map<String, *>): Fault =
        Fault(message, mergeCtx(getData(), ctx), cause)
}