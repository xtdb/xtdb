package xtdb.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Fault(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly(message, Anomaly.ensureCategory(data, FAULT), cause) {
    companion object {
        internal val FAULT = Keyword.intern("cognitect.anomalies", "fault")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Fault: $errorCode", dataFromMap(FAULT, errorCode, data), cause)
}