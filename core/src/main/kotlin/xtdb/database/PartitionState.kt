package xtdb.database

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.tx.OpenTx
import xtdb.arrow.RelationReader
import xtdb.block.proto.Block
import xtdb.catalog.TableCatalog
import xtdb.catalog.TableCatalog.Companion.latestBlock
import xtdb.indexer.LiveIndex
import xtdb.indexer.LiveTable
import xtdb.indexer.LiveSnap
import xtdb.indexer.ResolvedTx
import xtdb.indexer.Snapshot
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.TrieCatalog
import xtdb.trie.withTries
import xtdb.types.LogTimestamp
import xtdb.util.mapState
import xtdb.util.trace
import xtdb.util.closeAllOnCatch
import xtdb.table.fromSchemaAndTable
import xtdb.trie.TrieKey
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.requiringResolve
import xtdb.util.safelyOpening
import xtdb.util.withReadLock
import xtdb.util.withWriteLock
import java.time.Instant
import java.util.concurrent.locks.StampedLock

private val LOG = PartitionState::class.logger

/**
 * One partition's state: the one reference to its [PartitionValue], read-only views of its parts, and every write
 * to it — each one swap of the value.
 *
 * [snapLock] guards the live tables' Arrow memory, and nothing else: a write that closes a live table, or the live
 * snapshot, takes it for writing; a reader that takes references to them takes it for reading.
 */
class PartitionState private constructor(
    private val ref: MutableStateFlow<PartitionValue>,
    openLiveIndex: ((StateFlow<LiveIndex.State>) -> LiveIndex)?,
) : Snapshot.Source, AutoCloseable {
    /** A partition with no live index: its catalogs only. */
    constructor(initial: PartitionValue) : this(MutableStateFlow(initial), null)

    /** The partition's value, as each write leaves it. */
    internal val value: StateFlow<PartitionValue> get() = ref

    val tableCatalog = TableCatalog(ref.mapState { it.tableCat })
    val trieCatalog = TrieCatalog(ref.mapState { it.trieCat })
    val liveIndexOrNull: LiveIndex? = openLiveIndex?.invoke(ref.mapState { it.live })
    val liveIndex: LiveIndex get() = liveIndexOrNull ?: error("no live-index")

    private val snapLock = StampedLock()

    private val allocator get() = liveIndex.allocator

    fun addTries(table: TableRef, addedTries: Iterable<TrieDetails>, asOf: Instant) {
        LOG.trace { "Adding tries to table '$table': ${addedTries.map { it.trieKey }}" }
        try {
            ref.update { it.withTrieCat(it.trieCat.withTries(table, addedTries, asOf)) }
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Throwable) {
            LOG.error(e, "Failed to add tries to table $table")
            throw e
        }
    }

    /** Adds [tries] to whichever tables they name — the shape a replica message carries them in. */
    fun addTries(tries: Iterable<TrieDetails>, asOf: Instant) =
        tries.groupBy { it.tableName }.forEach { (tableName, tableTries) ->
            addTries(fromSchemaAndTable(tableName), tableTries, asOf)
        }

    fun deleteTries(table: TableRef, garbageTrieKeys: Set<TrieKey>) =
        ref.update { it.withTrieCat(it.trieCat.withoutTries(table, garbageTrieKeys)) }

    /** @see TrieCatalog.State.withL0Reset */
    fun resetToL0(tableToL0Entries: Map<TableRef, Any?>) =
        ref.update { it.withTrieCat(it.trieCat.withL0Reset(tableToL0Entries)) }

    /** A transient for [table], over the rows the live index holds for it now. */
    fun openTable(table: TableRef, systemTimeMicros: Long): LiveTable.Tx =
        snapLock.withReadLock { liveIndex.openTable(ref.value.live, table, systemTimeMicros) }

    /**
     * Installs each table's next value and releases the one it displaces, registering any table new to the
     * table catalog in the same write.
     *
     * [tables] is DRAINED once this partition has taken them, all together; if this throws before then, they
     * are still the caller's to free — which is why the caller hands the map over under `closeAllOnCatch`.
     *
     * The displaced value's buffers are its own references over memory the new value shares, so releasing
     * them frees nothing a reader still holds — every reader took its own slice (see [xtdb.indexer.TableSnapshot.open]).
     *
     * The leader's path for a transaction it resolved itself; one it reads back without having staged —
     * a promoted leader catching up — arrives as a deserialised relation and goes through [commitTx].
     */
    fun applyTx(txKey: TransactionKey, tables: MutableMap<TableRef, LiveTable>) = snapLock.withWriteLock {
        var live = ref.value.live
        val displaced = mutableListOf<LiveTable>()

        for ((table, liveTable) in tables) {
            live.tables[table]?.takeIf { it !== liveTable }?.let { displaced += it }
            live = live.withTable(table, liveTable)
        }
        live = live.withLatestCompletedTx(txKey)

        val next = live
        ref.update { it.withTableCat(it.tableCat.withEntriesFor(tables.keys)).withLive(next) }
        tables.clear()

        displaced.closeAll()
    }

    // Promote a committed tx into the live tables from the replica message's deserialised table data
    // (see `loadTableData`) — a follower, or a leader catching up. The caller owns [tables] and closes them afterwards.
    fun commitTx(txKey: TransactionKey, tables: Map<TableRef, RelationReader>) = snapLock.withWriteLock {
        val systemTimeMicros = txKey.systemTime.asMicros

        val before = ref.value.live

        mutableListOf<LiveTable>().closeAllOnCatch { imported ->
            var live = before
            for ((table, rel) in tables) {
                val next = liveIndex.imported(live, table, systemTimeMicros, rel).also { imported += it }
                live = live.withTable(table, next)
            }

            val next = live.withLatestCompletedTx(txKey)
            ref.update { it.withTableCat(it.tableCat.withEntriesFor(tables.keys)).withLive(next) }
        }

        tables.keys.mapNotNull { before.tables[it] }.closeAll()
    }

    /**
     * Adopt [block], in one write of the partition's value: register the L0 [tries] it carries, refresh the table
     * catalog onto it, roll the live index onto the block behind it, and drop the live snapshot, which holds the
     * finished block's tables. Those tables, and the partition's hold on the live snapshot, are released after that
     * write; a query still holding either keeps it until it closes.
     *
     * The caller MUST have made [block] durable first, because this is what moves the node past it — and
     * MUST call it once per block, the refresh's fold summing row counts.
     *
     * [logTimestamp] is the replica record's, dating the supersession the tries cause.
     */
    fun adoptBlock(block: Block, tries: List<TrieDetails>, logTimestamp: LogTimestamp) {
        val finished = ref.value
        val metadata = finished.live.blockMetadata()

        try {
            snapLock.withWriteLock {
                val displaced = ref.value
                check(displaced.live === finished.live) { "the live tables moved while adopting block ${block.blockIndex}" }

                ref.update { v ->
                    v.withTrieCat(v.trieCat.withTries(tries, logTimestamp))
                        .withTableCat(v.tableCat.refreshed(block, metadata))
                        .withLive(v.live.nextBlock())
                        .withLiveSnap(null)
                }
                displaced.releaseLive()
            }
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Throwable) {
            LOG.error(e, "Failed to adopt block ${block.blockIndex}, or to release its live tables")
            throw e
        }

        liveIndexOrNull?.afterNextBlock()
    }

    private fun LiveSnap.freshEnough(minSystemTime: Instant?) =
        minSystemTime == null || txBasis?.systemTime?.let { !it.isBefore(minSystemTime) } == true

    private fun PartitionValue.snapshotOver(liveSnap: LiveSnap): Snapshot {
        liveSnap.retain()
        return Snapshot(trieCat, tableCat, liveSnap)
    }

    /**
     * A snapshot of the partition's value: its catalogs as they stand, over a live snapshot whose basis is at least
     * [minSystemTime] — the value's own, or, where that lags, one built now from the value's live tables and kept in
     * the value for the queries after.
     */
    override fun openSnapshot(minSystemTime: Instant?): Snapshot {
        snapLock.withReadLock {
            val v = ref.value
            v.liveSnap?.let { if (it.freshEnough(minSystemTime)) return v.snapshotOver(it) }
        }

        return snapLock.withWriteLock {
            val v = ref.value
            v.liveSnap?.takeIf { it.freshEnough(minSystemTime) }?.let { return@withWriteLock v.snapshotOver(it) }

            val built = LiveSnap.open(allocator, v.live, emptyList(), null)
            ref.update { it.withLiveSnap(built) }
            v.liveSnap?.release()

            ref.value.snapshotOver(built)
        }
    }

    // The read lock spans the whole capture, not just the read of the value: it holds off block adoption and the
    // installs, which close the live tables the capture is slicing.
    fun openSnapshot(resolvedTxs: List<ResolvedTx>, ownTx: OpenTx): Snapshot =
        snapLock.withReadLock { ref.value.let { Snapshot.open(allocator, it.trieCat, it.tableCat, it.live, resolvedTxs, ownTx) } }

    // The live snapshot and the live tables of [this], once a write has replaced it.
    private fun PartitionValue.releaseLive() =
        (listOfNotNull(liveSnap?.let { AutoCloseable(it::release) }) + live.tables.values).closeAll()

    override fun close() {
        liveIndexOrNull?.let { liveIndex ->
            try {
                snapLock.withWriteLock {
                    val closing = ref.value
                    ref.update { it.withLive(LiveIndex.State(closing.live.blockIdx, closing.live.latestCompletedTx)).withLiveSnap(null) }
                    closing.releaseLive()
                }
            } finally {
                liveIndex.close()
            }
        }
    }

    companion object {
        private val trieCatalogFactory =
            requiringResolve("xtdb.trie-catalog/->factory").invoke() as TrieCatalog.Factory

        @JvmStatic
        @JvmOverloads
        fun open(
            allocator: BufferAllocator,
            bufferPool: BufferPool,
            indexerConfig: IndexerConfig = IndexerConfig(),
        ): PartitionState {
            val tableCatState = TableCatalog.loadState(bufferPool, bufferPool.latestBlock)
                // xt.txs and xt.role_membership are data-backed, so they're absent from the catalog
                // until the first transaction / GRANT. Seed them (as empty CREATE TABLEs) so they're
                // always resolvable - the columns mirror `OpenTx.writeTxRow` / the GRANT path.
                .withSeededTable(TableRef("xt", "txs"), listOf("_id", "system_time", "committed", "user_metadata", "error"))
                .withSeededTable(TableRef("xt", "role_membership"), listOf("user", "role"))

            return open(allocator, PartitionValue.loaded(trieCatalogFactory.open(bufferPool, tableCatState), tableCatState), indexerConfig)
        }

        /** A partition starting from [initial], with a live index over it. */
        @JvmStatic
        @JvmOverloads
        fun open(
            allocator: BufferAllocator,
            initial: PartitionValue,
            indexerConfig: IndexerConfig = IndexerConfig(),
            ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
        ): PartitionState = safelyOpening {
            open {
                PartitionState(MutableStateFlow(initial)) { flow ->
                    LiveIndex.open(allocator, flow, indexerConfig, ioDispatcher)
                }
            }
        }
    }
}
