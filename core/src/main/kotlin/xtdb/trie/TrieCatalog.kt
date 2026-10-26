package xtdb.trie

import xtdb.block.proto.Partition
import xtdb.catalog.TableCatalog
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.api.TableRef
import xtdb.table.fromSchemaAndTable
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.trace
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

typealias FileSize = Long

private val LOG = TrieCatalog::class.logger

class TrieCatalog(initialState: State) {
    private val stateRef = AtomicReference(initialState)

    /**
     * The catalog's current value, frozen — addTries / deleteTries that land afterward have no effect on it.
     *
     * Lifetime contract: a holder MUST be done with it within the configured `garbageLifetime` window,
     * otherwise GC may delete trie files it still references.
     */
    val state: State get() = stateRef.get()

    fun addTries(table: TableRef, addedTries: Iterable<TrieDetails>, asOf: Instant) {
        LOG.trace { "Adding tries to table '$table': ${addedTries.map { it.trieKey }}" }
        try {
            stateRef.updateAndGet { it.withTries(table, addedTries, asOf) }
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Throwable) {
            LOG.error(e, "Failed to add tries to table $table")
            throw e
        }
    }

    fun deleteTries(table: TableRef, garbageTrieKeys: Set<TrieKey>) {
        stateRef.updateAndGet { it.withoutTries(table, garbageTrieKeys) }
    }

    /** @see State.withL0Reset */
    fun resetToL0(tableToL0Entries: Map<TableRef, Any?>) {
        stateRef.updateAndGet { it.withL0Reset(tableToL0Entries) }
    }

    interface State {
        val tables: Set<TableRef>

        /** The raw per-table trie-state value (a Clojure persistent map) for downstream `current-tries` planning. */
        fun tableState(table: TableRef): Any?

        /**
         * The L0 watermark for [table] — the highest block index for which an L0 trie has been
         * published — or `-1` if no L0 partition exists yet.
         */
        fun l0MaxBlockIdx(table: TableRef): Long

        fun garbageTries(table: TableRef, asOf: Instant): Set<TrieKey>

        /**
         * Returns all garbage trie keys across all levels, regardless of garbage-as-of.
         * Not suitable for GC calculations — use [garbageTries] for that, which respects retention periods.
         */
        fun listAllGarbageTrieKeys(table: TableRef): Set<TrieKey>
        fun listAllTrieKeys(table: TableRef): List<TrieKey>
        fun listLiveAndNascentTrieKeys(table: TableRef): List<TrieKey>

        /**
         * The partition layout [table] would have with [addedTries] folded in.
         *
         * An empty [addedTries] gives the layout as it stands, and [asOf] dates the supersession the added
         * tries cause exactly as it does in [addTries].
         */
        fun withPartitions(table: TableRef, addedTries: Iterable<TrieDetails>, asOf: Instant): List<Partition>

        fun withTries(table: TableRef, addedTries: Iterable<TrieDetails>, asOf: Instant): State
        fun withoutTries(table: TableRef, garbageTrieKeys: Set<TrieKey>): State

        /**
         * DANGER: [this] with each table named in [tableToL0Entries] reset back to those L0s, higher levels wiped.
         * Each entry is a Clojure map with at least `:level 0`, `:block-idx`, `:trie-key`, `:recency nil`, `:part []`.
         */
        fun withL0Reset(tableToL0Entries: Map<TableRef, Any?>): State
    }

    fun interface Factory {
        fun open(bufferPool: BufferPool, tableCatalog: TableCatalog): State
    }
}

/**
 * Adds [tries] to whichever tables they name — the shape a replica message carries them in, where the
 * per-table [TrieCatalog.addTries] wants them already grouped.
 */
fun TrieCatalog.addTries(tries: Iterable<TrieDetails>, asOf: Instant) =
    tries.groupBy { it.tableName }.forEach { (tableName, tableTries) ->
        addTries(fromSchemaAndTable(tableName), tableTries, asOf)
    }
