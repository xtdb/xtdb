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

    inner class IidBranch(
        override val path: ByteArray, branchVecIdx: Int, private val recencyBound: RecencyMicros
    ) : Node, HashTrie.IidBranch<Leaf> {
        private val startIdx = iidBranchVec.getListStartIndex(branchVecIdx)
        private val count = iidBranchVec.getListCount(branchVecIdx)

        override val hashChildren: List<Node?>
            get() = List(count) { childBucket ->
                val childIdx = childBucket + startIdx
                if (iidBranchElVec.isNull(childIdx))
                    null
                else
                    forIndex(conjPath(path, childBucket.toByte()), iidBranchElVec.getInt(childIdx), recencyBound)
            }
    }

    inner class RecencyBranch(
        override val path: ByteArray, recencyBranchVec: VectorReader, branchVecIdx: Int,
        private val recencyBound: RecencyMicros
    ) : Node, HashTrie.RecencyBranch<Leaf> {
        private val startIdx = recencyBranchVec.getListStartIndex(branchVecIdx)
        private val count = recencyBranchVec.getListCount(branchVecIdx)
        private val recencyVec = recencyBranchVec.listElements[RECENCY]
        private val childIdxVec = recencyBranchVec.listElements[IDX]

        override val buckets: List<HashTrie.RecencyBranch.Bucket<Leaf>>
            get() = List(count) { bucket ->
                val elIdx = startIdx + bucket
                val recency = recencyVec.getLong(elIdx)
                HashTrie.RecencyBranch.Bucket(recency, forIndex(path, childIdxVec.getInt(elIdx), minOf(recencyBound, recency))!!)
            }
    }

    /**
     * @property recencyBound an inclusive upper bound on the recency of every row in the page -
     *   the tightest recency-branch key above it, or [Long.MAX_VALUE] if there's none
     */
    class Leaf(override val path: ByteArray, val dataPageIndex: Int, val recencyBound: RecencyMicros) :
        Node, HashTrie.Leaf<Leaf>

    private fun forIndex(path: ByteArray, idx: Int, recencyBound: RecencyMicros): Node? =
        when (nodesVec.getLeg(idx)) {
            NIL -> null
            BRANCH_IID -> IidBranch(path, idx, recencyBound)
            BRANCH_RECENCY -> RecencyBranch(path, recencyBranchVec!!, idx, recencyBound)
            LEAF -> Leaf(path, dataPageIdxVec.getInt(idx), recencyBound)
            else -> error("unknown leg: ${nodesVec.getLeg(idx)}")
        }

    override val rootNode
        get() = if (nodesVec.valueCount == 0) null else forIndex(ByteArray(0), nodesVec.valueCount - 1, Long.MAX_VALUE)

    override fun close() {
        if (closeNodesVec) nodesVec.close()
    }
}

