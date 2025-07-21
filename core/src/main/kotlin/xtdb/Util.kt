package xtdb

import clojure.lang.Keyword
import clojure.lang.Symbol
import java.nio.ByteBuffer
import java.util.*

val UUID.asByteBuffer: ByteBuffer
    get() = ByteBuffer.wrap(ByteArray(16)).apply {
        putLong(mostSignificantBits)
        putLong(leastSignificantBits)
        flip()
    }

val UUID.asBytes: ByteArray get() = asByteBuffer.array()

val String.kw: Keyword get() = Keyword.intern(this)

val String.symbol: Symbol get() = Symbol.intern(this)

fun Map<*, *>.toClojureMap(): clojure.lang.IPersistentMap =
    clojure.lang.PersistentHashMap.create(this)
