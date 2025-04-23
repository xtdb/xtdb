package xtdb.trie

import xtdb.log.proto.TrieDetails

typealias FileSize = Long

interface TrieCatalog {
    fun addTries(tableName: TableName, addedTries: Iterable<TrieDetails>)
    val tableNames: Set<String>
}
