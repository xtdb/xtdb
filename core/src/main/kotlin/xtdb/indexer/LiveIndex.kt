package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.api.TransactionKey
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveTable.Companion.finishBlock
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.BlockIndex
import xtdb.trie.ColumnName
import xtdb.trie.MemoryHashTrie
import xtdb.trie.TrieCatalog
import xtdb.util.RowCounter
import xtdb.util.RefCounter
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.logger
import xtdb.util.safelyOpening
import xtdb.util.warn
import java.io.ByteArrayInputStream
import java.nio.channels.Channels
import java.time.Duration
import java.util.HashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.StampedLock

private val LOG = LiveIndex::class.logger

// Deserialize a replica-log [ReplicaMessage.ResolvedTx]'s per-table IPC bytes back into owned
// relations, for the follower/transition path which only has the serialized form. The leader skips
// this entirely — it commits from the relations it already holds. The caller closes the result.
internal fun ReplicaMessage.ResolvedTx.loadTableData(al: BufferAllocator): Map<TableRef, Relation> =
    mutableMapOf<TableRef, Relation>().closeAllOnCatch { rels ->
        for ((schemaAndTable, ipcBytes) in tableData) {
            Relation.StreamLoader(al, Channels.newChannel(ByteArrayInputStream(ipcBytes))).use { loader ->
                Relation(al, loader.schema).closeOnCatch { rel ->
                    loader.loadNextPage(rel)
                    rels[TableRef.parse(schemaAndTable)] = rel
                }
            }
        }
        rels
    }

class LiveIndex private constructor(
    private val allocator: BufferAllocator,
    private val tableCatalog: TableCatalog,
    val trieCatalog: TrieCatalog,
    private val dbName: String,
    @Volatile var latestCompletedTx: TransactionKey?,
    initialBlockIdx: Long,
    indexerConfig: IndexerConfig,
) : Snapshot.Source, AutoCloseable {

    private val tables = HashMap<TableRef, LiveTable>()

    /**
     * The block index that data committed *now* belongs to — handed to each [LiveTable] on creation.
     * Bumped under [snapLock]'s write lock in [nextBlock] so subsequent commit/import paths see the new value.
     */
    @Volatile
    var blockIdx: Long = initialBlockIdx
        private set

    @Volatile
    private var sharedSnap: Snapshot? = null
    private val snapLock = StampedLock()
    private val snapRefCounter = RefCounter()
    private val rowCounter = RowCounter()

    @JvmField
    val logLimit = indexerConfig.logLimit

    @JvmField
    val pageLimit = indexerConfig.pageLimit

    private val rowsPerBlock = indexerConfig.rowsPerBlock
    private val skipTxs = indexerConfig.skipTxs

    private val liveTrieFactory = LiveTable.LiveTrieFactory { iidVec ->
        MemoryHashTrie.builder(iidVec)
            .setLogLimit(logLimit.toInt())
            .setPageLimit(pageLimit.toInt())
            .build()
    }

    private fun refreshSnap0() {
        val oldSnap = sharedSnap

        sharedSnap = Snapshot.open(allocator, tableCatalog, trieCatalog, this)

        oldSnap?.close()
    }

    /**
     * Rebuilds the cached shared snapshot to pick up out-of-band mutations to the captured
     * catalogs (notably the [TrieCatalog]) that didn't go through this live-index's own
     * commit/import/block paths.
     *
     * Takes the snap write-lock itself; callers MUST NOT already hold it.
     */
    fun refreshSnap() {
        val stamp = snapLock.writeLock()
        try {
            refreshSnap0()
        } finally {
            snapLock.unlock(stamp)
        }
    }

    fun table(table: TableRef): LiveTable? = this@LiveIndex.tables[table]
    val tableRefs: Iterable<TableRef> get() = this@LiveIndex.tables.keys

    // Promote a committed tx into the live tables straight from its relations — no IPC round-trip.
    // The leader passes its staged relation slices; a follower deserializes the replica message's
    // table data first (see `loadTableData`). The caller owns [tables] and closes them afterwards.
    fun commitTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>) {
        val stamp = snapLock.writeLock()
        try {
            for ((ref, rel) in tables) {
                val liveTable =
                    this@LiveIndex.tables.getOrPut(ref) {
                        LiveTable(allocator, ref, blockIdx, rowCounter, liveTrieFactory)
                    }
                liveTable.importData(rel)
            }

            latestCompletedTx = txKey

            refreshSnap0()
        } finally {
            snapLock.unlock(stamp)
        }
    }

    private inline fun <R> StampedLock.withReadLock(block: () -> R): R {
        val stamp = readLock()
        return try {
            block()
        } finally {
            unlock(stamp)
        }
    }

    override fun openSnapshot() =
        snapLock.withReadLock {
            sharedSnap!!.also { it.retain() }
        }

    fun openSnapshot(stagedTxs: List<StagedTx>, ownTx: OpenTx): Snapshot =
        snapLock.withReadLock {
            // Hold the snap read-lock for the whole capture: it blocks `nextBlock` (write-lock) so live
            // tables can't be cleared mid-iteration, and it brackets the trie-cat snapshot so we can't
            // end up with a stale (no-L0_N) trie-cat alongside a live-tables view that's already been
            // reset past N. `addTries` doesn't take the snap lock — it's allowed to land on either side
            // of our trie-cat capture without breaking correctness.
            Snapshot.open(allocator, tableCatalog, trieCatalog, this, stagedTxs, ownTx)
        }

    fun isFull() = rowCounter.blockRowCount >= rowsPerBlock

    fun blockMetadata(): Map<TableRef, LiveTable.BlockMetadata> =
        this@LiveIndex.tables.mapValues { (_, lt) -> lt.blockMetadata() }

    fun finishBlock(bp: BufferPool, blockIdx: BlockIndex) = this@LiveIndex.tables.finishBlock(bp, blockIdx)

    private val skipTxsLogged = AtomicBoolean(false)

    fun nextBlock() {
        rowCounter.nextBlock()

        val stamp = snapLock.writeLock()
        try {
            this@LiveIndex.tables.values.closeAll()
            this@LiveIndex.tables.clear()

            blockIdx += 1L

            refreshSnap0()
        } finally {
            snapLock.unlock(stamp)
        }

        if (skipTxs.isNotEmpty() && latestCompletedTx != null
            && latestCompletedTx!!.txId >= skipTxs.last()
            && skipTxsLogged.compareAndSet(false, true)
        ) {
            LOG.warn("All XTDB_SKIP_TXS have been skipped and block has been finished - it is safe to remove the XTDB_SKIP_TXS environment variable.")
        }
    }

    override fun close() {
        sharedSnap?.close()
        this@LiveIndex.tables.values.closeAll()
        if (!snapRefCounter.tryClose(Duration.ofMinutes(1)))
            LOG.warn("Failed to shut down live-index after 60s due to outstanding watermarks.")
        else
            allocator.close()
    }

    companion object {
        @JvmStatic
        @JvmOverloads
        @JvmName("open")
        fun open(
            allocator: BufferAllocator, blockCatalog: BlockCatalog, tableCatalog: TableCatalog,
            trieCatalog: TrieCatalog, dbName: String, indexerConfig: IndexerConfig = IndexerConfig(),
        ) = safelyOpening {
            LiveIndex(
                open { allocator.newChildAllocator("live-index", 0, Long.MAX_VALUE) },
                tableCatalog, trieCatalog, dbName,
                blockCatalog.latestCompletedTx,
                (blockCatalog.currentBlockIndex ?: -1L) + 1L,
                indexerConfig,
            ).also { it.refreshSnap() }
        }
    }
}
