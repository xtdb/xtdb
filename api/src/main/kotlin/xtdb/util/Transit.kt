package xtdb.util

import clojure.lang.Keyword

enum class TransitFormat(fmt: String) {
    JSON("json"), MSGPACK("msgpack");

    val key: Keyword = Keyword.intern(fmt)
}

fun readTransit(bs: ByteArray, fmt: TransitFormat): Any? =
    requiringResolve("xtdb.serde/read-transit").invoke(bs, fmt.key)

fun writeTransit(obj: Any, fmt: TransitFormat): ByteArray =
    requiringResolve("xtdb.serde/write-transit").invoke(obj, fmt.key) as ByteArray
