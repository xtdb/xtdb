package xtdb.api.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Incorrect(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly.Caller(message, ensureCategory(data, INCORRECT), cause) {

    companion object {
        internal val INCORRECT = Keyword.intern("cognitect.anomalies/incorrect")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Incorrect: $errorCode", dataFromMap(INCORRECT, errorCode, data), cause)

    override fun mergeCtx(ctx: Map<String, *>): Incorrect =
        Incorrect(message, mergeCtx(getData(), ctx), cause)
}