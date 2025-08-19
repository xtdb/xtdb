package xtdb.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class NotFound(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly.Caller(message, data, cause) {
    companion object {
        internal val NOT_FOUND = Keyword.intern("cognitect.anomalies", "not-found")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Not found: $errorCode", dataFromMap(NOT_FOUND, errorCode, data), cause)

}