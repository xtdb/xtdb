package xtdb.trie

import xtdb.block.proto.Partition
import xtdb.catalog.Catalog
import xtdb.catalog.TableCatalog
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.api.TableRef
import xtdb.table.fromSchemaAndTable
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.trace
import java.time.Instant

typealias FileSize = Long

private val LOG = TrieCatalog::class.logger

/** The trie half of a [Catalog]. */
class TrieCatalog internal constructor(private val catalog: Catalog) {
    private fun update(f: (State) -> State) = catalog.update { it.mapTrieCat(f) }

    /**
     * The catalog's current value, frozen — addTries / deleteTries that land afterward have no effect on it.
     *
     * Lifetime contract: a holder MUST be done with it within the configured `garbageLifetime` window,
     * otherwise GC may delete trie files it still references.
     */
    val state: State get() = catalog.current.trieCat

    fun addTries(table: TableRef, addedTries: Iterable<TrieDetails>, asOf: Instant) =
        logFailure("table $table") {
            LOG.trace { "Adding tries to table '$table': ${addedTries.map { it.trieKey }}" }
            update { it.withTries(table, addedTries, asOf) }
        }

    /** Adds [tries] to whichever tables they name, as one write — the shape a replica message carries them in. */
    fun addTries(tries: Iterable<TrieDetails>, asOf: Instant) =
        logFailure("tables ${tries.map { it.tableName }.distinct()}") {
            LOG.trace { "Adding tries: ${tries.map { it.trieKey }}" }
            update { it.withTries(tries, asOf) }
        }

    private inline fun logFailure(what: String, f: () -> Unit) {
        try {
            f()
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Throwable) {
            LOG.error(e, "Failed to add tries to $what")
            throw e
        }
    }

    fun deleteTries(table: TableRef, garbageTrieKeys: Set<TrieKey>) {
        update { it.withoutTries(table, garbageTrieKeys) }
    }

    /** @see State.withL0Reset */
    fun resetToL0(tableToL0Entries: Map<TableRef, Any?>) {
        update { it.withL0Reset(tableToL0Entries) }
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
        fun open(bufferPool: BufferPool, tableCatState: TableCatalog.State): State
    }
}

fun TrieCatalog.State.withTries(tries: Iterable<TrieDetails>, asOf: Instant): TrieCatalog.State =
    tries.groupBy { it.tableName }.entries.fold(this) { state, (tableName, tableTries) ->
        state.withTries(fromSchemaAndTable(tableName), tableTries, asOf)
    }
