package xtdb.trie

import xtdb.vector.IRelationWriter
import xtdb.vector.RelationReader
import java.util.*

typealias InstantMicros = Long
typealias RowIndex = Int

interface ITrieWriter : AutoCloseable {
    val dataWriter: IRelationWriter

    fun writeLeaf(): RowIndex
    fun writeIidBranch(idxs: IntArray): RowIndex
    fun writeRecencyBranch(idxBuckets: SortedMap<InstantMicros, RowIndex>, subTrieData: RelationReader?): RowIndex

    fun end()

    override fun close()
}
