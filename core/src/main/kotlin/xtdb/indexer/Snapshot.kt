package xtdb.indexer

import xtdb.api.TransactionKey
import java.util.concurrent.atomic.AtomicInteger

class Snapshot(
    val txBasis: TransactionKey?,
    val liveIndex: LiveIndex.Snapshot?,
    val schema: Map<String, Any>
) : AutoCloseable {
    interface Source {
        fun openSnapshot(): Snapshot
    }

    private val refCount = AtomicInteger(1)

    fun retain() {
        if (0 == refCount.getAndIncrement()) throw IllegalStateException("snapshot closed")
    }

    /**
     * releases a reference to the Snapshot.
     * if this was the last reference, close it.
     */
    override fun close() {
        if (0 == refCount.decrementAndGet()) liveIndex?.close()
    }
}
