package xtdb.trie

import xtdb.arrow.Vector

private const val NIL = "nil"
private const val BRANCH_IID = "branch-iid"
private const val BRANCH_RECENCY = "branch-recency"
private const val LEAF = "leaf"
private const val DATA_PAGE_IDX = "data-page-idx"

class ArrowHashTrie(private val nodesVec: Vector) :
    HashTrie<ArrowHashTrie.Node, ArrowHashTrie.Leaf> {

    private val iidBranchVec = nodesVec.legReader(BRANCH_IID)!!
    private val iidBranchElVec = iidBranchVec.elementReader()

    private val recencyBranchVec = nodesVec.legReader(BRANCH_RECENCY)!!
    private val recencyVec = recencyBranchVec.mapKeyReader()
    private val recencyIdxVec = recencyBranchVec.mapValueReader()

    private val dataPageIdxVec = nodesVec.legReader(LEAF)!!.keyReader(DATA_PAGE_IDX)!!

    interface Node : HashTrie.Node<Node>

    inner class IidBranch(override val path: ByteArray, branchVecIdx: Int) : Node {
        private val startIdx = iidBranchVec.getListStartIndex(branchVecIdx)
        private val count = iidBranchVec.getListCount(branchVecIdx)

        override val iidChildren: Array<Node?>
            get() = Array(count) { childBucket ->
                val childIdx = childBucket + startIdx
                if (iidBranchElVec.isNull(childIdx))
                    null
                else
                    forIndex(conjPath(path, childBucket.toByte()), iidBranchElVec.getInt(childIdx))
            }

        override val recencies = null
        override fun recencyNode(idx: Int) = throw UnsupportedOperationException()
    }

    inner class RecencyBranch(override val path: ByteArray, branchVecIdx: Int) : Node {

        override val iidChildren = null

        private val startIdx = recencyBranchVec.getListStartIndex(branchVecIdx)
        private val count = recencyBranchVec.getListCount(branchVecIdx)

        override val recencies: RecencyArray
            get() = RecencyArray(count) { idx ->
                recencyVec.getLong(idx + startIdx)
            }

        override fun recencyNode(idx: Int) =
            checkNotNull(forIndex(path, recencyIdxVec.getInt(startIdx + idx)))

    }

    inner class Leaf(override val path: ByteArray, private val leafOffset: Int) : Node {
        val dataPageIndex get() = dataPageIdxVec.getInt(leafOffset)

        override val iidChildren = null
        override val recencies = null
        override fun recencyNode(idx: Int) = throw UnsupportedOperationException()
    }

    private fun forIndex(path: ByteArray, idx: Int) =
        when (nodesVec.getLeg(idx)) {
            NIL -> null
            BRANCH_IID -> IidBranch(path, idx)
            BRANCH_RECENCY -> RecencyBranch(path, idx)
            LEAF -> Leaf(path, idx)
            else -> error("unknown leg: ${nodesVec.getLeg(idx)}")
        }

    override val rootNode get() = forIndex(ByteArray(0), nodesVec.valueCount - 1)
}

