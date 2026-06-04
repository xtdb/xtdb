package xtdb.database

import xtdb.api.log.Watchers
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.indexer.LiveIndex
import xtdb.indexer.Snapshot
import xtdb.trie.TrieCatalog

class DatabasePartition(
    val partition: Int,
    val state: DatabaseState,
    val watchers: Watchers,
    val compactorOrNull: Compactor.ForDatabase? = null,
) : AutoCloseable {

    val blockCatalog: BlockCatalog get() = state.blockCatalog
    val tableCatalog: TableCatalog get() = state.tableCatalog
    val trieCatalog: TrieCatalog get() = state.trieCatalog
    val liveIndex: LiveIndex get() = state.liveIndex

    val compactor: Compactor.ForDatabase
        get() = compactorOrNull ?: error("compactor not initialised")

    fun openSnapshot(): Snapshot = state.liveIndex.openSnapshot()

    // The compactor is no longer closed here — its driver is freed when the compactor job
    // completes (cancelled via the Database's compactorScope), not by an AutoCloseable call.
    override fun close() {
        state.close()
    }
}
