package xtdb.util

import clojure.lang.Keyword

internal enum class TransitFormat(fmt: String) {
    JSON("json"), MSGPACK("msgpack");

    val key: Keyword = Keyword.intern(fmt)
}

internal fun readTransit(bs: ByteArray, fmt: TransitFormat) =
    requiringResolve("xtdb.serde/read-transit").invoke(bs, fmt.key)

internal fun writeTransit(obj: Any, fmt: TransitFormat) =
    requiringResolve("xtdb.serde/write-transit").invoke(obj, fmt.key) as ByteArray
