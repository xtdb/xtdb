package xtdb.trie

import xtdb.arrow.Vector
import xtdb.arrow.VectorReader

private const val NIL = "nil"
private const val BRANCH_IID = "branch-iid"
private const val BRANCH_RECENCY = "branch-recency"
private const val LEAF = "leaf"
private const val DATA_PAGE_IDX = "data-page-idx"
private const val RECENCY = "recency"
private const val IDX = "idx"

class ArrowHashTrie @JvmOverloads constructor(
    private val nodesVec: Vector, private val closeNodesVec: Boolean = false
) : HashTrie<ArrowHashTrie.Leaf>, AutoCloseable {

    private val iidBranchVec = nodesVec[BRANCH_IID]
    private val iidBranchElVec = iidBranchVec.listElements

    private val recencyBranchVec = nodesVec.vectorForOrNull(BRANCH_RECENCY)

    private val dataPageIdxVec = nodesVec[LEAF][DATA_PAGE_IDX]

    sealed interface Node : HashTrie.Node<Leaf>

    inner class IidBranch(override val path: ByteArray, branchVecIdx: Int) : Node, HashTrie.IidBranch<Leaf> {
        private val startIdx = iidBranchVec.getListStartIndex(branchVecIdx)
        private val count = iidBranchVec.getListCount(branchVecIdx)

        override val hashChildren: List<Node?>
            get() = List(count) { childBucket ->
                val childIdx = childBucket + startIdx
                if (iidBranchElVec.isNull(childIdx))
                    null
                else
                    forIndex(conjPath(path, childBucket.toByte()), iidBranchElVec.getInt(childIdx))
            }
    }

    inner class RecencyBranch(
        override val path: ByteArray, recencyBranchVec: VectorReader, branchVecIdx: Int
    ) : Node, HashTrie.RecencyBranch<Leaf> {
        private val startIdx = recencyBranchVec.getListStartIndex(branchVecIdx)
        private val count = recencyBranchVec.getListCount(branchVecIdx)
        private val recencyVec = recencyBranchVec.listElements[RECENCY]
        private val childIdxVec = recencyBranchVec.listElements[IDX]

        override val buckets: List<HashTrie.RecencyBranch.Bucket<Leaf>>
            get() = List(count) { bucket ->
                val elIdx = startIdx + bucket
                HashTrie.RecencyBranch.Bucket(recencyVec.getLong(elIdx), forIndex(path, childIdxVec.getInt(elIdx))!!)
            }
    }

    class Leaf(override val path: ByteArray, val dataPageIndex: Int) : Node, HashTrie.Leaf<Leaf>

    private fun forIndex(path: ByteArray, idx: Int): Node? =
        when (nodesVec.getLeg(idx)) {
            NIL -> null
            BRANCH_IID -> IidBranch(path, idx)
            BRANCH_RECENCY -> RecencyBranch(path, recencyBranchVec!!, idx)
            LEAF -> Leaf(path, dataPageIdxVec.getInt(idx))
            else -> error("unknown leg: ${nodesVec.getLeg(idx)}")
        }

    override val rootNode
        get() = if (nodesVec.valueCount == 0) null else forIndex(ByteArray(0), nodesVec.valueCount - 1)

    override fun close() {
        if (closeNodesVec) nodesVec.close()
    }
}

