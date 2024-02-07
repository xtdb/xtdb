package xtdb.trie

import xtdb.vector.IRelationWriter

interface ITrieWriter : AutoCloseable {
    val dataWriter: IRelationWriter

    fun writeLeaf(): Int
    fun writeBranch(idxs: IntArray): Int
    
    fun end()

    override fun close()
}
