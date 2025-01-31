package xtdb.trie

import xtdb.log.proto.AddedTrie

interface TrieCatalog {
    fun addTrie(addedTrie: AddedTrie)
}

