package xtdb.trie

import xtdb.vector.IRelationWriter
import java.util.*

typealias InstantMicros = Long
typealias RowIndex = Int

interface ITrieWriter : AutoCloseable {
    val dataWriter: IRelationWriter

    fun writeLeaf(): RowIndex
    fun writeIidBranch(idxs: IntArray): RowIndex
    fun writeRecencyBranch(idxBuckets: SortedMap<InstantMicros, RowIndex>): RowIndex

    fun end()

    override fun close()
}
