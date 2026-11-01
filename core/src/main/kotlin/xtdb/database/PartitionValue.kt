package xtdb.database

import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveIndex
import xtdb.indexer.LiveSnap
import xtdb.trie.TrieCatalog

/**
 * What one database partition knows, as one value: every write replaces it whole, through the one
 * reference [PartitionState] holds.
 *
 * Not a data class: a reference that compares values on update would walk every table's tries.
 */
class PartitionValue internal constructor(
    val trieCat: TrieCatalog.State,
    val tableCat: TableCatalog.State,
    internal val live: LiveIndex.State,
    /**
     * The latest live snapshot built — the live tables' slices that every external snapshot opened over this
     * value shares. It may lag [live]: a commit carries it forward, and a query that needs a newer one builds it.
     * Absent until a query needs one, and after a block's adoption, so that one held here is always the current
     * block's.
     *
     * The one resource in the value: a write that carries it forward neither retains nor releases it, and only
     * the write that replaces it releases the old one.
     */
    internal val liveSnap: LiveSnap?,
) {
    internal fun withTrieCat(trieCat: TrieCatalog.State) = PartitionValue(trieCat, tableCat, live, liveSnap)
    internal fun withTableCat(tableCat: TableCatalog.State) = PartitionValue(trieCat, tableCat, live, liveSnap)
    internal fun withLive(live: LiveIndex.State) = PartitionValue(trieCat, tableCat, live, liveSnap)
    internal fun withLiveSnap(liveSnap: LiveSnap?) = PartitionValue(trieCat, tableCat, live, liveSnap)

    companion object {
        /** A partition's value as its catalogs load it: the live index empty, on the block after the catalogs' latest. */
        @JvmStatic
        fun loaded(trieCat: TrieCatalog.State, tableCat: TableCatalog.State) =
            PartitionValue(
                trieCat, tableCat,
                LiveIndex.State((tableCat.blockIdx ?: -1L) + 1L, tableCat.block?.latestCompletedTx),
                liveSnap = null,
            )
    }
}
