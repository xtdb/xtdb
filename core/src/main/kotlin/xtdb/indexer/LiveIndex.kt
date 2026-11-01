package xtdb.indexer

import clojure.lang.IPersistentMap
import clojure.lang.PersistentHashMap
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.api.TransactionKey
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveTable.Companion.finishBlock
import xtdb.storage.BufferPool
import xtdb.api.TableRef
import xtdb.table.fromSchemaAndTable
import xtdb.trie.BlockIndex
import xtdb.trie.MemoryHashTrie
import xtdb.trie.TrieCatalog
import xtdb.time.InstantUtil.asMicros
import xtdb.util.RefCounter
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.logger
import xtdb.util.safelyOpening
import xtdb.util.warn
import xtdb.util.withReadLock
import xtdb.util.withWriteLock
import java.nio.channels.Channels
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.StampedLock
import xtdb.api.tx.OpenTx

private val LOG = LiveIndex::class.logger

// Deserialize a replica-log [ReplicaMessage.ResolvedTx]'s per-table IPC bytes back into owned
// relations, for the follower/transition path which only has the serialized form. The leader skips
// this entirely — it commits from the relations it already holds. The caller closes the result.
internal fun ReplicaMessage.ResolvedTx.loadTableData(al: BufferAllocator): Map<TableRef, Relation> =
    mutableMapOf<TableRef, Relation>().closeAllOnCatch { rels ->
        for ((schemaAndTable, ipcBytes) in tableData) {
            Relation.StreamLoader(al, Channels.newChannel(ipcBytes.newInput())).use { loader ->
                Relation(al, loader.schema).closeOnCatch { rel ->
                    loader.loadNextPage(rel)
                    rels[fromSchemaAndTable(schemaAndTable)] = rel
                }
            }
        }
        rels
    }

class LiveIndex private constructor(
    private val allocator: BufferAllocator,
    private val tableCatalog: TableCatalog,
    val trieCatalog: TrieCatalog,
    initialLatestCompletedTx: TransactionKey?,
    initialBlockIdx: Long,
    indexerConfig: IndexerConfig,
    private val ioDispatcher: CoroutineDispatcher,
) : Snapshot.Source, AutoCloseable {

    /**
     * The live tables, the block they belong to, the rows that block has gained and the latest transaction
     * applied into them, as one value: replaced whole under [snapLock]'s write lock.
     */
    class State private constructor(
        private val tableMap: IPersistentMap,
        val blockIdx: Long,
        val blockRowCount: Long,
        val latestCompletedTx: TransactionKey?,
    ) {
        internal constructor(blockIdx: Long, latestCompletedTx: TransactionKey?) :
                this(PersistentHashMap.EMPTY, blockIdx, 0, latestCompletedTx)

        @Suppress("UNCHECKED_CAST")
        val tables: Map<TableRef, LiveTable> get() = tableMap as Map<TableRef, LiveTable>

        fun table(table: TableRef): LiveTable? = tables[table]

        fun blockMetadata(): Map<TableRef, LiveTable.BlockMetadata> =
            tables.mapValues { (_, lt) -> lt.blockMetadata() }

        /**
         * [table] in place of whatever [ref] held, counting the rows the block gained off the two values rather
         * than accumulating them: a value carries every row below it, so this stays right even where the value it
         * displaces is several transactions behind — a stale tx skipped on the way in, say.
         */
        internal fun withTable(ref: TableRef, table: LiveTable): State {
            val addedRows = table.relation.rowCount - (tables[ref]?.relation?.rowCount ?: 0)
            return State(tableMap.assoc(ref, table), blockIdx, blockRowCount + addedRows, latestCompletedTx)
        }

        internal fun withLatestCompletedTx(txKey: TransactionKey) = State(tableMap, blockIdx, blockRowCount, txKey)

        internal fun nextBlock() = State(blockIdx + 1, latestCompletedTx)
    }

    @Volatile
    var state = State(initialBlockIdx, initialLatestCompletedTx)
        private set

    @Volatile
    private var sharedSnap: Snapshot? = null
    private val snapLock = StampedLock()
    private val snapRefCounter = RefCounter()

    @JvmField
    val logLimit = indexerConfig.logLimit

    @JvmField
    val pageLimit = indexerConfig.pageLimit

    /** The block-size threshold. Public because the leader cuts blocks off its own resolve-side row
     * gauge (which leads the applied count [blockRowCount] reflects) and must use the same limit. */
    val rowsPerBlock = indexerConfig.rowsPerBlock

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
        snapLock.withWriteLock {
            refreshSnap0()
        }
    }

    /**
     * A transient for [table], over the rows the live index holds for it now.
     *
     * A table nobody has committed to yet gets one over a relation of its own, rather than an empty
     * [LiveTable] entered here: the map is what resolution reads a table's existence from, so a table
     * entered at resolve would be visible to queries before the transaction creating it applied.
     */
    fun openTable(table: TableRef, systemTimeMicros: Long): LiveTable.Tx =
        snapLock.withReadLock {
            val state = state
            state.table(table)?.openTx(systemTimeMicros)
            // The empty value is opened and dropped rather than entered: the transient holds its own
            // references over the same memory, so closing the value it came from frees nothing it needs.
                ?: LiveTable.open(allocator, table, tableCatalog.snap().slug(table), state.blockIdx, liveTrieFactory)
                    .use { it.openTx(systemTimeMicros) }
        }

    /**
     * Installs each table's next value and releases the one it displaces.
     *
     * [tables] is DRAINED: an entry leaves it at the moment this index takes the value, so whatever is
     * still in it on return is still the caller's to free. That is what makes a throw part-way through
     * survivable — every value is owned by exactly one side rather than by neither — and it is why the
     * caller hands the map over under `closeAllOnCatch` rather than after it.
     *
     * The displaced value's buffers are its own references over memory the new value shares, so releasing
     * them frees nothing a reader still holds — every reader took its own slice (see [TableSnapshot.open]).
     *
     * The leader's path for a transaction it resolved itself; one it reads back without having staged —
     * a promoted leader catching up — arrives as a deserialised relation and goes through [commitTx].
     */
    fun applyTx(txKey: TransactionKey, tables: MutableMap<TableRef, LiveTable>) {
        snapLock.withWriteLock {
            tableCatalog.registerTables(tables.keys)

            val iter = tables.iterator()
            while (iter.hasNext()) {
                val (ref, liveTable) = iter.next()

                // Installing is what can fail — the next state allocates — and it fails before it has taken anything,
                // so at that instant the value is still the caller's and their guard frees it. The
                // removal that follows cannot fail, so the value is never reachable from neither.
                val displaced = state.tables[ref]
                state = state.withTable(ref, liveTable)
                iter.remove()

                if (displaced !== liveTable) displaced?.close()
            }

            state = state.withLatestCompletedTx(txKey)
        }
    }

    // Promote a committed tx into the live tables from the replica message's deserialised table data
    // (see `loadTableData`) — a follower, or a leader catching up. The caller owns [tables] and closes them afterwards.
    fun commitTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>) {
        snapLock.withWriteLock {
            // Inside the lock, so a table's identity and its rows reach readers together — snapshot
            // capture brackets itself with the same lock. Whole batch at once, per `registerTables`.
            tableCatalog.registerTables(tables.keys)

            val systemTimeMicros = txKey.systemTime.asMicros

            for ((ref, rel) in tables) {
                val displaced = state.tables[ref]
                val next = displaced?.importData(systemTimeMicros, rel)
                    // Pinned at creation, so the L0 trie this table writes at the block boundary lands
                    // under the same slug `BlockCutter` then records for it.
                    ?: LiveTable.open(allocator, ref, tableCatalog.snap().slug(ref), state.blockIdx, liveTrieFactory)
                        .use { it.importData(systemTimeMicros, rel) }

                next.closeOnCatch { state = state.withTable(ref, it) }
                if (displaced !== next) displaced?.close()
            }

            state = state.withLatestCompletedTx(txKey)
        }
    }

    private fun Snapshot.freshEnough(minSystemTime: Instant?) =
        minSystemTime == null || txBasis?.systemTime?.let { !it.isBefore(minSystemTime) } == true

    override fun openSnapshot(minSystemTime: Instant?): Snapshot {
        snapLock.withReadLock {
            sharedSnap!!.let { if (it.freshEnough(minSystemTime)) return it.also { s -> s.retain() } }
        }

        // cache too stale for the caller's basis — rebuild under the write lock, double-checking first
        snapLock.withWriteLock {
            if (!sharedSnap!!.freshEnough(minSystemTime)) refreshSnap0()
            return sharedSnap!!.also { it.retain() }
        }
    }

    fun openSnapshot(resolvedTxs: List<ResolvedTx>, ownTx: OpenTx): Snapshot =
        snapLock.withReadLock {
            // Hold the snap read-lock for the whole capture: it blocks `nextBlock` (write-lock) so live
            // tables can't be cleared mid-iteration, and it brackets the trie-cat snapshot so we can't
            // end up with a stale (no-L0_N) trie-cat alongside a live-tables view that's already been
            // reset past N. `addTries` doesn't take the snap lock — it's allowed to land on either side
            // of our trie-cat capture without breaking correctness.
            Snapshot.open(allocator, tableCatalog, trieCatalog, this, resolvedTxs, ownTx)
        }

    fun isFull() = state.blockRowCount >= rowsPerBlock

    suspend fun finishBlock(bp: BufferPool, blockIdx: BlockIndex) =
        state.tables.finishBlock(bp, blockIdx, ioDispatcher)

    private val skipTxsLogged = AtomicBoolean(false)

    fun nextBlock() {
        snapLock.withWriteLock {
            val finished = state
            finished.tables.values.closeAll()
            state = finished.nextBlock()
        }

        val latestCompletedTx = state.latestCompletedTx
        if (skipTxs.isNotEmpty() && latestCompletedTx != null
            && latestCompletedTx.txId >= skipTxs.last()
            && skipTxsLogged.compareAndSet(false, true)
        ) {
            LOG.warn("All XTDB_SKIP_TXS have been skipped and block has been finished - it is safe to remove the XTDB_SKIP_TXS environment variable.")
        }
    }

    override fun close() {
        sharedSnap?.close()

        // Emptied as well as closed, as `nextBlock` does: these relations are slices sharing memory with
        // readers' own, so releasing one a second time underflows a ledger somebody else is still holding.
        val closing = state
        closing.tables.values.closeAll()
        state = State(closing.blockIdx, closing.latestCompletedTx)

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
            allocator: BufferAllocator, tableCatalog: TableCatalog,
            trieCatalog: TrieCatalog, indexerConfig: IndexerConfig = IndexerConfig(),
            /**
             * Dispatcher for the per-table block-write fan-out. Sims inject the seeded dispatcher so the
             * fan-out stays on the simulation's thread, and hence within its quiescence fixed point.
             */
            ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
        ) = safelyOpening {
            val tableCat = tableCatalog.snap()
            LiveIndex(
                open { allocator.newChildAllocator("live-index", 0, Long.MAX_VALUE) },
                tableCatalog, trieCatalog,
                tableCat.latestCompletedTx,
                (tableCat.blockIdx ?: -1L) + 1L,
                indexerConfig,
                ioDispatcher,
            ).also { it.refreshSnap() }
        }
    }
}
