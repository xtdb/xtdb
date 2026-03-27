package xtdb.trie

import xtdb.block.proto.Partition
import xtdb.catalog.BlockCatalog
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.table.TableRef
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

    fun interface Factory {
        fun openTrieCatalog(bufferPool: BufferPool, blockCatalog: BlockCatalog): TrieCatalog
    }
}
