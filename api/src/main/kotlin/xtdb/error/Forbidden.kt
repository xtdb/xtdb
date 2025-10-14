package xtdb.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Forbidden(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly.Caller(message, Anomaly.ensureCategory(data, FORBIDDEN), cause) {
    companion object {
        internal val FORBIDDEN = Keyword.intern("cognitect.anomalies", "forbidden")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Forbidden: $errorCode", dataFromMap(FORBIDDEN, errorCode, data), cause)

}