package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.catalog.BlockCatalog
import xtdb.catalog.BlockCatalog.Companion.latestBlock
import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveIndex
import xtdb.table.DatabaseName
import xtdb.trie.TrieCatalog
import xtdb.util.requiringResolve
import xtdb.util.safelyOpening

data class DatabaseState(
    val name: DatabaseName,
    val blockCatalogOrNull: BlockCatalog?,
    val tableCatalogOrNull: TableCatalog?,
    val trieCatalogOrNull: TrieCatalog?,
    val liveIndexOrNull: LiveIndex?,
) : AutoCloseable {
    val blockCatalog: BlockCatalog get() = blockCatalogOrNull ?: error("no block-catalog")
    val tableCatalog: TableCatalog get() = tableCatalogOrNull ?: error("no table-catalog")
    val trieCatalog: TrieCatalog get() = trieCatalogOrNull ?: error("no trie-catalog")
    val liveIndex: LiveIndex get() = liveIndexOrNull ?: error("no live-index")

    override fun close() {
        liveIndexOrNull?.close()
    }

    companion object {
        private val trieCatalogFactory =
            requiringResolve("xtdb.trie-catalog/->factory").invoke() as TrieCatalog.Factory

        @JvmStatic
        @JvmOverloads
        fun open(
            allocator: BufferAllocator,
            storage: DatabaseStorage,
            dbName: DatabaseName,
            indexerConfig: IndexerConfig = IndexerConfig(),
        ): DatabaseState = safelyOpening {
            val bufferPool = storage.bufferPool

            val blockCatalog = BlockCatalog(dbName, bufferPool.latestBlock)

            val tableCatalog = TableCatalog(blockCatalog, bufferPool).also {
                it.refresh(blockCatalog.currentBlockIndex ?: -1)
            }

            val trieCatalog = trieCatalogFactory.open(bufferPool, blockCatalog)

            val liveIndex = open { LiveIndex.open(allocator, blockCatalog, tableCatalog, dbName, indexerConfig) }

            DatabaseState(dbName, blockCatalog, tableCatalog, trieCatalog, liveIndex)
        }
    }
}
