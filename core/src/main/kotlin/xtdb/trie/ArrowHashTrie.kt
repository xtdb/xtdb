package xtdb.trie

import xtdb.arrow.Vector

private const val NIL = "nil"
private const val BRANCH_IID = "branch-iid"
private const val LEAF = "leaf"
private const val DATA_PAGE_IDX = "data-page-idx"

class ArrowHashTrie(private val nodesVec: Vector) : HashTrie<ArrowHashTrie.Node, ArrowHashTrie.Leaf> {

    private val iidBranchVec = nodesVec[BRANCH_IID]
    private val iidBranchElVec = iidBranchVec.listElements

    private val dataPageIdxVec = nodesVec[LEAF][DATA_PAGE_IDX]

    interface Node : HashTrie.Node<Node>

    inner class IidBranch(override val path: ByteArray, branchVecIdx: Int) : Node {
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

    class Leaf(override val path: ByteArray, val dataPageIndex: Int) : Node {
        override val hashChildren = null
    }

    private fun forIndex(path: ByteArray, idx: Int) =
        when (nodesVec.getLeg(idx)) {
            NIL -> null
            BRANCH_IID -> IidBranch(path, idx)
            LEAF -> Leaf(path, dataPageIdxVec.getInt(idx))
            else -> error("unknown leg: ${nodesVec.getLeg(idx)}")
        }

    override val rootNode get() = forIndex(ByteArray(0), nodesVec.valueCount - 1)
}

