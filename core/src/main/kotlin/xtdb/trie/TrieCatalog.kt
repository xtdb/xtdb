package xtdb.trie

interface TrieCatalog {
    fun addTrie(tableName: String, trieKey: String)

    val tableNames: Collection<String>

    fun currentTries(tableName: String): Collection<String>
}