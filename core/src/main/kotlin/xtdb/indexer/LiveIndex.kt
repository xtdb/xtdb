package xtdb.indexer

import clojure.lang.IPersistentMap
import clojure.lang.PersistentHashMap
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.StateFlow
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.api.TransactionKey
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.indexer.LiveTable.Companion.finishBlock
import xtdb.storage.BufferPool
import xtdb.api.TableRef
import xtdb.table.BlockMetadata
import xtdb.table.TableSlug
import xtdb.table.fromSchemaAndTable
import xtdb.trie.BlockIndex
import xtdb.trie.MemoryHashTrie
import xtdb.util.RefCounter
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.logger
import xtdb.util.safelyOpening
import xtdb.util.warn
import java.nio.channels.Channels
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

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

/**
 * A partition's live index: the resources its live tables are built with, and reads of the live part of the
 * partition's value through [flow]. It knows nothing of the catalogs; [xtdb.database.PartitionState] combines
 * the two, and makes every write to the value.
 */
class LiveIndex private constructor(
    internal val allocator: BufferAllocator,
    private val flow: StateFlow<State>,
    indexerConfig: IndexerConfig,
    private val ioDispatcher: CoroutineDispatcher,
) : AutoCloseable {

    /**
     * The live tables, the block they belong to, the rows that block has gained and the latest transaction
     * applied into them, as one value.
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

        fun blockMetadata(): Map<TableRef, BlockMetadata> =
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

    val state: State get() = flow.value

    private val snapRefCounter = RefCounter()

    @JvmField
    val logLimit = indexerConfig.logLimit

    @JvmField
    val pageLimit = indexerConfig.pageLimit

    /** The block-size threshold. Public because the leader cuts blocks off its own resolve-side row
     * gauge (which leads the applied count [State.blockRowCount] reflects) and must use the same limit. */
    val rowsPerBlock = indexerConfig.rowsPerBlock

    private val skipTxs = indexerConfig.skipTxs

    private val liveTrieFactory = LiveTable.LiveTrieFactory { iidVec ->
        MemoryHashTrie.builder(iidVec)
            .setLogLimit(logLimit.toInt())
            .setPageLimit(pageLimit.toInt())
            .build()
    }

    /**
     * A transient for [table], over the rows [state] holds for it.
     *
     * A table nobody has committed to yet gets one over a relation of its own, rather than an empty
     * [LiveTable] entered here: the map is what resolution reads a table's existence from, so a table
     * entered at resolve would be visible to queries before the transaction creating it applied.
     */
    internal fun openTable(state: State, table: TableRef, systemTimeMicros: Long): LiveTable.Tx =
        state.table(table)?.openTx(systemTimeMicros)
        // The empty value is opened and dropped rather than entered: the transient holds its own
        // references over the same memory, so closing the value it came from frees nothing it needs.
            ?: LiveTable.open(allocator, table, state.blockIdx, liveTrieFactory)
                .use { it.openTx(systemTimeMicros) }

    /** [table]'s next value in [state], with [rel]'s rows imported — the caller owns both [rel] and the result. */
    internal fun imported(state: State, table: TableRef, systemTimeMicros: Long, rel: RelationReader): LiveTable =
        state.table(table)?.importData(systemTimeMicros, rel)
            ?: LiveTable.open(allocator, table, state.blockIdx, liveTrieFactory)
                .use { it.importData(systemTimeMicros, rel) }

    fun isFull() = state.blockRowCount >= rowsPerBlock

    suspend fun finishBlock(bp: BufferPool, blockIdx: BlockIndex, slug: (TableRef) -> TableSlug) =
        state.tables.finishBlock(bp, blockIdx, slug, ioDispatcher)

    private val skipTxsLogged = AtomicBoolean(false)

    internal fun afterNextBlock() {
        val latestCompletedTx = state.latestCompletedTx
        if (skipTxs.isNotEmpty() && latestCompletedTx != null
            && latestCompletedTx.txId >= skipTxs.last()
            && skipTxsLogged.compareAndSet(false, true)
        ) {
            LOG.warn("All XTDB_SKIP_TXS have been skipped and block has been finished - it is safe to remove the XTDB_SKIP_TXS environment variable.")
        }
    }

    override fun close() {
        if (!snapRefCounter.tryClose(Duration.ofMinutes(1)))
            LOG.warn("Failed to shut down live-index after 60s due to outstanding watermarks.")
        else
            allocator.close()
    }

    companion object {
        @JvmStatic
        @JvmOverloads
        @JvmName("open")
        internal fun open(
            allocator: BufferAllocator, flow: StateFlow<State>, indexerConfig: IndexerConfig = IndexerConfig(),
            /**
             * Dispatcher for the per-table block-write fan-out. Sims inject the seeded dispatcher so the
             * fan-out stays on the simulation's thread, and hence within its quiescence fixed point.
             */
            ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
        ) = safelyOpening {
            LiveIndex(
                open { allocator.newChildAllocator("live-index", 0, Long.MAX_VALUE) },
                flow, indexerConfig, ioDispatcher,
            )
        }
    }
}
