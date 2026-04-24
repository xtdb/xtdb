package xtdb

import clojure.lang.Keyword
import clojure.lang.Symbol
import xtdb.util.warn
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.util.*
import kotlin.coroutines.cancellation.CancellationException

private val LOG: System.Logger = System.getLogger("xtdb.util")

val UUID.asByteBuffer: ByteBuffer
    get() = ByteBuffer.wrap(ByteArray(16)).apply {
        putLong(mostSignificantBits)
        putLong(leastSignificantBits)
        flip()
    }

val UUID.asBytes: ByteArray get() = asByteBuffer.array()

val String.kw: Keyword get() = Keyword.intern(this)

val String.symbol: Symbol get() = Symbol.intern(this)

fun Throwable.isCancellation() =
    this is InterruptedException || this is CancellationException || this is ClosedByInterruptException

fun Throwable.rethrowIfCancellation() {
    when (this) {
        is CancellationException -> throw this
        is InterruptedException -> throw CancellationException(this)
        is ClosedByInterruptException -> {
            if (!Thread.interrupted()) LOG.warn(this, "caught ClosedByInterruptException with cleared thread interrupted status!!!")
            throw CancellationException(this)
        }
    }
}
