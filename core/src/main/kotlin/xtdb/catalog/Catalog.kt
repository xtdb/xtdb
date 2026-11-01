package xtdb.catalog

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import xtdb.api.TableRef
import xtdb.block.proto.Block
import xtdb.indexer.LiveTable
import xtdb.log.proto.TrieDetails
import xtdb.table.TableEntry
import xtdb.trie.TrieCatalog
import xtdb.trie.TrieKey
import xtdb.table.fromSchemaAndTable
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.trace
import java.time.Instant
import java.util.concurrent.locks.StampedLock
import java.util.function.Function

private val LOG = Catalog::class.logger

/**
 * What one database partition knows about its tables and their tries, as one value, and the lock that
 * brackets it against the partition's live tables.
 *
 * Every write goes through [update], under the lock the live index also takes for its live tables, so a
 * reader under that lock sees the catalog value and the live tables as of the same moment.
 * [tableCatalog] and [trieCatalog] are the read views.
 */
class Catalog(initialState: State) {

    class State(val trieCat: TrieCatalog.State, val tableCat: TableCatalog.State) {

        internal fun mapTableCat(f: (TableCatalog.State) -> TableCatalog.State): State =
            f(tableCat).let { if (it === tableCat) this else State(trieCat, it) }

        internal fun mapTrieCat(f: (TrieCatalog.State) -> TrieCatalog.State): State =
            f(trieCat).let { if (it === trieCat) this else State(it, tableCat) }

        /**
         * Gives an entry to each of [tables] that hasn't one, so that a table has its identity from the moment
         * the catalog learns of it rather than from its first block.
         *
         * Allocating in name order is what lets a follower agree with the leader: a transaction's tables reach
         * this as an unordered map, so arrival order within one transaction differs between them, while the
         * order transactions themselves arrive does not.
         *
         * A call naming only known tables returns this value.
         */
        fun withEntriesFor(tables: Collection<TableRef>) = mapTableCat { it.withEntriesFor(tables) }

        /**
         * The registry for a block covering [tables], each of which MUST already have an entry — sorted, so
         * the same table set yields the same block bytes whichever node wrote it.
         */
        fun entriesFor(tables: Collection<TableRef>): List<TableEntry> =
            tables.sortedBy { it.schemaAndTable }.map { tableCat.entriesByTable.getValue(it) }

        /**
         * Advances to a newly durable block, folding that block's per-table metadata in with it.
         *
         * A re-delivered block leaves the block half alone but still folds the metadata.
         */
        fun refreshed(block: Block?, metadata: Map<TableRef, LiveTable.BlockMetadata> = emptyMap()) =
            mapTableCat { it.refreshed(block, metadata) }

        fun withTries(table: TableRef, addedTries: Iterable<TrieDetails>, asOf: Instant): State {
            LOG.trace { "Adding tries to table '$table': ${addedTries.map { it.trieKey }}" }
            try {
                return mapTrieCat { it.withTries(table, addedTries, asOf) }
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Throwable) {
                LOG.error(e, "Failed to add tries to table $table")
                throw e
            }
        }

        fun withTries(tries: Iterable<TrieDetails>, asOf: Instant): State =
            tries.groupBy { it.tableName }.entries.fold(this) { s, (tableName, tableTries) ->
                s.withTries(fromSchemaAndTable(tableName), tableTries, asOf)
            }

        fun withoutTries(table: TableRef, garbageTrieKeys: Set<TrieKey>) =
            mapTrieCat { it.withoutTries(table, garbageTrieKeys) }

        fun withL0Reset(tableToL0Entries: Map<TableRef, Any?>) =
            mapTrieCat { it.withL0Reset(tableToL0Entries) }
    }

    private val lock = StampedLock()
    private val stateFlow = MutableStateFlow(initialState)

    /** The latest value, read without the lock — for a reader that wants no more than this one value. */
    val current: State get() = stateFlow.value

    internal val flow: StateFlow<State> get() = stateFlow

    internal val isWriteLocked: Boolean get() = lock.isWriteLocked

    // Neither block can suspend — a lambda that isn't inline can't make a suspending call — so the lock is
    // never held across a suspension point.

    /**
     * Publishes what [f] returns from the value current now, and returns it.
     * [f] MAY change the live index's live tables alongside: it runs under the lock they're guarded by.
     *
     * NOT reentrant: [f] MUST NOT call [update] or [withReadLock].
     */
    fun update(f: Function<State, State>): State {
        val stamp = lock.writeLock()
        try {
            return f.apply(stateFlow.value).also { stateFlow.value = it }
        } finally {
            lock.unlockWrite(stamp)
        }
    }

    /**
     * For the live index, whose live tables this lock also guards — any other reader wants [current].
     *
     * NOT reentrant: [f] MUST NOT call [update] or [withReadLock] — a queued writer would deadlock it.
     */
    internal fun <R> withReadLock(f: Function<State, R>): R {
        val stamp = lock.readLock()
        try {
            return f.apply(stateFlow.value)
        } finally {
            lock.unlockRead(stamp)
        }
    }

    val tableCatalog = TableCatalog(this)
    val trieCatalog = TrieCatalog(this)
}
