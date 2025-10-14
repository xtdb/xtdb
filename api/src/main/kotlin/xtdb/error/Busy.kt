package xtdb.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Busy(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly(message, Anomaly.ensureCategory(data, BUSY), cause) {
    companion object {
        internal val BUSY = Keyword.intern("cognitect.anomalies", "busy")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Busy: $errorCode", dataFromMap(BUSY, errorCode, data), cause)


}