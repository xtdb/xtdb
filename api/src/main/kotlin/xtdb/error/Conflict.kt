package xtdb.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Conflict(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly.Caller(message, Anomaly.ensureCategory(data, CONFLICT), cause) {

    companion object {
        internal val CONFLICT = Keyword.intern("cognitect.anomalies", "conflict")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Conflict: $errorCode", dataFromMap(CONFLICT, errorCode, data), cause)
}