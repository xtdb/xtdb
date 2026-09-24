package xtdb.database

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.block.proto.Block
import xtdb.catalog.TableCatalog
import xtdb.catalog.TableCatalog.Companion.latestBlock
import xtdb.indexer.LiveIndex
import xtdb.storage.BufferPool
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

    /**
     * Where this partition records its transactions.
     *
     * Tx-ids are per-partition counters, so at more than one partition every partition would write
     * `_id = 0, 1, 2…` into one table and XTDB would read those as one entity at several times. Resolved
     * by whoever knows the partition count — see [txsTableFor].
     */
    val txsTable: TableRef = TXS,
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

        private val TXS = TableRef("xt", "txs")

        /**
         * A single-partition database keeps `xt.txs` exactly as it is, so an existing user's
         * `SELECT * FROM xt.txs` survives an upgrade with no migration (#5836).
         */
        @JvmStatic
        fun txsTableFor(partition: Int, partitionCount: Int) =
            if (partitionCount == 1) TXS else TableRef("xt", "txs_$partition")

        @JvmStatic
        @JvmOverloads
        fun open(
            allocator: BufferAllocator,
            bufferPool: BufferPool,
            indexerConfig: IndexerConfig = IndexerConfig(),
            txsTable: TableRef = TXS,
        ): PartitionState = safelyOpening {
            val tableCatalog = TableCatalog(bufferPool, bufferPool.latestBlock).also {
                it.loadTables()
                // xt.txs and xt.role_membership are data-backed, so they're absent from the catalog
                // until the first transaction / GRANT. Seed them (as empty CREATE TABLEs) so they're
                // always resolvable - the columns mirror `OpenTx.writeTxRow` / the GRANT path. On a node
                // that already has these tables, the loaded types win (no-op seed).
                it.seedTable(txsTable, listOf("_id", "system_time", "committed", "user_metadata", "error"))
                it.seedTable(TableRef("xt", "role_membership"), listOf("user", "role"))
            }

            val trieCatalog = trieCatalogFactory.open(bufferPool, tableCatalog)

            val liveIndex = open { LiveIndex.open(allocator, tableCatalog, trieCatalog, indexerConfig) }

            PartitionState(tableCatalog, trieCatalog, liveIndex, txsTable)
        }
    }
}
