package xtdb.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Unsupported(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly.Caller(message, data, cause) {
    companion object {
        internal val UNSUPPORTED = Keyword.intern("cognitect.anomalies", "unsupported")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Unsupported: $errorCode", dataFromMap(UNSUPPORTED, errorCode, data), cause)

}