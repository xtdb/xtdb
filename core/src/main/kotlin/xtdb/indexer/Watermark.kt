package xtdb.indexer

import xtdb.api.TransactionKey
import java.util.concurrent.atomic.AtomicInteger

class Watermark(
    val txBasis: TransactionKey?,
    val liveIndex: LiveIndex.Watermark?,
    val schema: Map<String, Any>
) : AutoCloseable {
    interface Source {
        fun openWatermark(): Watermark
    }

    private val refCount = AtomicInteger(1)

    fun retain() {
        if (0 == refCount.getAndIncrement()) throw IllegalStateException("watermark closed")
    }

    /**
     * releases a reference to the Watermark.
     * if this was the last reference, close it.
     */
    override fun close() {
        if (0 == refCount.decrementAndGet()) liveIndex?.close()
    }
}
