package xtdb.database

import xtdb.api.log.Watchers
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.indexer.LiveIndex
import xtdb.indexer.LogProcessor
import xtdb.indexer.Snapshot
import xtdb.trie.TrieCatalog
import java.time.Instant

class DatabasePartition(
    val partition: Int,
    val state: DatabaseState,
    val watchers: Watchers,
    val compactorOrNull: Compactor.ForDatabase? = null,
    val logProcessor: LogProcessor? = null,
) : AutoCloseable {

    val blockCatalog: BlockCatalog get() = state.blockCatalog
    val tableCatalog: TableCatalog get() = state.tableCatalog
    val trieCatalog: TrieCatalog get() = state.trieCatalog
    val liveIndex: LiveIndex get() = state.liveIndex

    val compactor: Compactor.ForDatabase
        get() = compactorOrNull ?: error("compactor not initialised")

    fun openSnapshot(minSystemTime: Instant?): Snapshot = state.liveIndex.openSnapshot(minSystemTime)

    override fun close() {
        logProcessor?.close()
        compactorOrNull?.close()
        state.close()
    }
}
