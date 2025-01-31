package xtdb.trie

import xtdb.arrow.Relation
import java.util.*

typealias InstantMicros = Long
typealias RowIndex = Int

interface TrieWriter : AutoCloseable {
    val dataRel: Relation

    fun writeLeaf(): RowIndex
    fun writeIidBranch(idxs: IntArray): RowIndex
    fun writeRecencyBranch(idxBuckets: SortedMap<InstantMicros, RowIndex>): RowIndex

    /**
     * @return the size of the data file
     */
    fun end(): FileSize

    override fun close()
}
