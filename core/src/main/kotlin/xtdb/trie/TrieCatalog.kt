package xtdb.trie

import xtdb.log.proto.TrieDetails

typealias FileSize = Long

interface TrieCatalog {
    fun addTries(addedTries: Iterable<TrieDetails>)
    val tableNames: Set<String>
}
