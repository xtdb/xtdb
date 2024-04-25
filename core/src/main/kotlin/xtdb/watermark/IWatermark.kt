package xtdb.watermark

import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.TransactionKey
import xtdb.trie.LiveHashTrie
import xtdb.vector.RelationReader
import java.util.concurrent.atomic.AtomicInteger

interface ILiveTableWatermark : AutoCloseable {
    fun columnField(col: String): Field
    fun columnFields(): Map<String, Field>
    fun liveRelation(): RelationReader
    fun liveTrie(): LiveHashTrie
}

interface ILiveIndexWatermark : AutoCloseable {
    fun allColumnFields(): Map<String, Map<String, Field>>
    fun liveTable(tableName: String): ILiveTableWatermark
}

class Watermark(
    @JvmField val txBasis: TransactionKey?,
    @JvmField val liveIndex: ILiveIndexWatermark?,
) : AutoCloseable {
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

interface IWatermarkSource {
    fun openWatermark(): Watermark
}
