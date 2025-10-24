package xtdb.trie

import xtdb.log.proto.TrieDetails
import xtdb.table.TableRef
import java.time.Instant

typealias FileSize = Long

interface TrieCatalog {
    fun addTries(table: TableRef, addedTries: Iterable<TrieDetails>, asOf: Instant)
    val tables: Set<TableRef>
    fun garbageTries(table: TableRef, asOf: Instant) : Set<TrieKey>
    fun deleteTries(table: TableRef, garbageTrieKeys: Set<TrieKey>)
    fun listAllTrieKeys(table: TableRef) : List<TrieKey>
}
