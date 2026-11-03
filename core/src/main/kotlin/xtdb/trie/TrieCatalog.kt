package xtdb.trie

import kotlinx.coroutines.flow.StateFlow
import xtdb.block.proto.Partition
import xtdb.catalog.TableCatalog
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.api.TableRef
import xtdb.table.fromSchemaAndTable
import java.time.Instant

typealias FileSize = Long

/** A partition's trie catalog: a read view of the trie part of its value, which [xtdb.database.PartitionState] writes. */
class TrieCatalog(private val flow: StateFlow<State>) {

    /**
     * The catalog's current value, frozen — writes that land afterward have no effect on it.
     *
     * Lifetime contract: a holder MUST be done with it within the configured `garbageLifetime` window,
     * otherwise GC may delete trie files it still references.
     */
    val state: State get() = flow.value

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
         * tries cause exactly as it does in [withTries].
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

/** [this], with [tries] added to whichever tables they name — the shape a replica message carries them in. */
fun TrieCatalog.State.withTries(tries: Iterable<TrieDetails>, asOf: Instant): TrieCatalog.State =
    tries.groupBy { it.tableName }.entries.fold(this) { state, (tableName, tableTries) ->
        state.withTries(fromSchemaAndTable(tableName), tableTries, asOf)
    }
