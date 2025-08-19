package xtdb.error

import clojure.lang.IPersistentMap
import clojure.lang.Keyword
import clojure.lang.PersistentHashMap

class Interrupted(
    message: String?, data: IPersistentMap = PersistentHashMap.EMPTY, cause: Throwable? = null
) : Anomaly(message, data, cause){
    companion object {
        internal val INTERRUPTED = Keyword.intern("cognitect.anomalies", "interrupted")
    }

    constructor(
        message: String? = null, errorCode: String? = null, data: Map<String, *>? = null, cause: Throwable? = null
    ) : this(message ?: "Interrupted", dataFromMap(INTERRUPTED, errorCode, data), cause)
}