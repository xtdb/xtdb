package xtdb.trie

import xtdb.log.proto.TrieDetails
import java.time.Instant

typealias FileSize = Long

interface TrieCatalog {
    fun addTries(tableName: TableName, addedTries: Iterable<TrieDetails>, asOf: Instant)
    val tableNames: Set<String>
    fun garbageTries(tableName: TableName, asOf: Instant) : Set<TrieKey>
    fun deleteTries(tableName: TableName, garbageTrieKeys: Set<TrieKey>)
}
