package xtdb.table

import xtdb.arrow.FieldName
import xtdb.arrow.VectorType
import xtdb.log.proto.TrieMetadata
import xtdb.trie.FileSize
import xtdb.trie.TrieKey
import xtdb.util.HLL

data class BlockMetadata(
    val vecTypes: Map<FieldName, VectorType>,
    val rowCount: Int,
    val hllDeltas: Map<FieldName, HLL>
)

data class FinishedBlock(
    val vecTypes: Map<FieldName, VectorType>,
    val rowCount: Int,
    val hllDeltas: Map<FieldName, HLL>,
    /**
     * Null exactly when [rowCount] is zero.
     *
     * A table can be staged by a transaction without taking any rows from it — `CREATE TABLE`,
     * or DML whose predicate matched nothing — and it still has to reach the table catalog so
     * that its declared columns survive. It has no trie to write, though, and an empty L0 costs
     * two objects and a trie-catalog entry for nothing.
     */
    val writtenTrie: WrittenTrie?
) {
    data class WrittenTrie(
        val trieKey: TrieKey,
        val dataFileSize: FileSize,
        val trieMetadata: TrieMetadata
    )
}
