package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.api.log.LeaderTerm
import xtdb.catalog.TableCatalog
import xtdb.catalog.TableCatalog.Companion.latestBlock
import xtdb.indexer.LiveIndex
import xtdb.indexer.TermFence
import xtdb.api.TableRef
import xtdb.trie.TrieCatalog
import xtdb.util.requiringResolve
import xtdb.util.safelyOpening

class PartitionState(
    val tableCatalogOrNull: TableCatalog?,
    val trieCatalogOrNull: TrieCatalog?,
    val liveIndexOrNull: LiveIndex?,
) : AutoCloseable {
    val tableCatalog: TableCatalog get() = tableCatalogOrNull ?: error("no table-catalog")
    val trieCatalog: TrieCatalog get() = trieCatalogOrNull ?: error("no trie-catalog")
    val liveIndex: LiveIndex get() = liveIndexOrNull ?: error("no live-index")

    /**
     * Seeded from the persisted block boundary, then only ever raised — so it outlives every role
     * change on this partition. See [TermFence].
     */
    val termFence = TermFence(tableCatalogOrNull?.boundaryTermId ?: LeaderTerm.NONE)

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
            storage: PartitionStorage,
            indexerConfig: IndexerConfig = IndexerConfig(),
        ): PartitionState = safelyOpening {
            val bufferPool = storage.bufferPool

            val tableCatalog = TableCatalog(bufferPool, bufferPool.latestBlock).also {
                it.loadTables()
                // xt.txs and xt.role_membership are data-backed, so they're absent from the catalog
                // until the first transaction / GRANT. Seed them (as empty CREATE TABLEs) so they're
                // always resolvable - the columns mirror `OpenTx.writeTxRow` / the GRANT path. On a node
                // that already has these tables, the loaded types win (no-op seed).
                it.seedTable(TableRef("xt", "txs"), listOf("_id", "system_time", "committed", "user_metadata", "error"))
                it.seedTable(TableRef("xt", "role_membership"), listOf("user", "role"))
            }

            val trieCatalog = trieCatalogFactory.open(bufferPool, tableCatalog)

            val liveIndex = open { LiveIndex.open(allocator, tableCatalog, trieCatalog, indexerConfig) }

            PartitionState(tableCatalog, trieCatalog, liveIndex)
        }
    }
}
