package xtdb.trie

import xtdb.block.proto.Partition
import xtdb.catalog.BlockCatalog
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.api.TableRef
import java.time.Instant

typealias FileSize = Long

interface TrieCatalog {
    fun addTries(table: TableRef, addedTries: Iterable<TrieDetails>, asOf: Instant)
    val tables: Set<TableRef>
    fun garbageTries(table: TableRef, asOf: Instant) : Set<TrieKey>
    /**
     * Returns all garbage trie keys across all levels, regardless of garbage-as-of.
     * Not suitable for GC calculations — use [garbageTries] for that, which respects retention periods.
     */
    fun listAllGarbageTrieKeys(table: TableRef): Set<TrieKey>
    fun deleteTries(table: TableRef, garbageTrieKeys: Set<TrieKey>)
    fun listAllTrieKeys(table: TableRef) : List<TrieKey>
    fun listLiveAndNascentTrieKeys(table: TableRef) : List<TrieKey>
    fun getPartitions(table: TableRef): List<Partition>

    /**
     * Captures a frozen view of every table's trie state in a single shallow copy of the per-table map.
     * Per-table values are immutable Clojure persistent maps, so each table's state is already frozen
     * the moment the [Snap] is taken — addTries / deleteTries that land afterward have no effect on it.
     *
     * Snapshot lifetime contract: callers MUST close their [xtdb.indexer.Snapshot] within the configured
     * `garbageLifetime` window, otherwise GC may delete trie files the snap still references.
     */
    fun snapshot(): Snap

    interface Snap {
        val tables: Set<TableRef>
        /** The raw per-table trie-state value (a Clojure persistent map) for downstream `current-tries` planning. */
        fun tableState(table: TableRef): Any?
        /**
         * The L0 watermark for [table] — the highest block index for which an L0 trie has been
         * published — or `-1` if no L0 partition exists yet.
         */
        fun l0MaxBlockIdx(table: TableRef): Long
    }

    fun interface Factory {
        fun open(bufferPool: BufferPool, blockCatalog: BlockCatalog): TrieCatalog
    }
}
