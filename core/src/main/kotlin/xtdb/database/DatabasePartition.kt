package xtdb.database

import xtdb.api.log.Watchers
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.indexer.LiveIndex
import xtdb.api.tx.ExternalSource
import xtdb.indexer.LogProcessor
import xtdb.indexer.Snapshot
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import java.time.Instant

class DatabasePartition(
    val storage: PartitionStorage,
    val state: PartitionState,
    val watchers: Watchers,
    val compactorOrNull: Compactor.ForDatabase? = null,
    val logProcessor: LogProcessor? = null,
    val externalSource: ExternalSource? = null,
) : AutoCloseable {

    // the storage view owns the partition index; delegate rather than store a second copy so the two can't disagree
    val partition: Int get() = storage.partition

    val tableCatalog: TableCatalog get() = state.tableCatalog
    val trieCatalog: TrieCatalog get() = state.trieCatalog
    val liveIndex: LiveIndex get() = state.liveIndex
    val bufferPool: BufferPool get() = storage.bufferPool
    val metadataManager: PageMetadata.Factory get() = storage.metadataManager

    val compactor: Compactor.ForDatabase
        get() = compactorOrNull ?: error("compactor not initialised")

    fun openSnapshot(minSystemTime: Instant?): Snapshot = state.liveIndex.openSnapshot(minSystemTime)

    override fun close() {
        // The source outlives every leader term, so it goes after the processor that borrows it.
        logProcessor?.close()
        externalSource?.close()
        compactorOrNull?.close()
        state.close()
        storage.close()
    }
}
