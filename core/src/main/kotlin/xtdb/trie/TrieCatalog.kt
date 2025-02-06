package xtdb.trie

import xtdb.log.proto.AddedTrie

typealias FileSize = Long

interface TrieCatalog {
    fun addTries(addedTrie: Iterable<AddedTrie>)
}
