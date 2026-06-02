package xtdb.database

import xtdb.api.log.Watchers
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.indexer.LiveIndex
import xtdb.indexer.Snapshot
import xtdb.trie.TrieCatalog
import xtdb.util.closeAll

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

    override fun close() {
        listOf(compactorOrNull, state).closeAll()
    }
}
