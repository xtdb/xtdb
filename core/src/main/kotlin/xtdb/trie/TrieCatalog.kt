package xtdb.trie

interface TrieCatalog {
    fun addTrie(tableName: String, trieKey: String)
}