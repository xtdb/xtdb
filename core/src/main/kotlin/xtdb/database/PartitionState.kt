package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.block.proto.Block
import xtdb.catalog.TableCatalog
import xtdb.catalog.TableCatalog.Companion.latestBlock
import xtdb.indexer.LiveIndex
import xtdb.api.TableRef
import xtdb.log.proto.TrieDetails
import xtdb.trie.TrieCatalog
import xtdb.trie.addTries
import xtdb.types.LogTimestamp
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
     * Adopt [block]: register the L0 [tries] it carries, refresh the table catalog onto it, and roll the
     * live index onto the block behind it.
     *
     * The caller MUST have made [block] durable first, because this is what moves the node past it — and
     * MUST call it once per block, the refresh's fold summing row counts.
     *
     * [logTimestamp] is the replica record's, dating the supersession the tries cause.
     */
    fun adoptBlock(block: Block, tries: List<TrieDetails>, logTimestamp: LogTimestamp) {
        trieCatalog.addTries(tries, logTimestamp)

        // `blockMetadata()` reads the live tables that `nextBlock()` then clears.
        tableCatalog.refresh(block, liveIndex.blockMetadata())
        liveIndex.nextBlock()
    }

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
