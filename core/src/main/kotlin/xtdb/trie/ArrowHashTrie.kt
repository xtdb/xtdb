package xtdb.trie

import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.StructVector
import xtdb.vector.ValueVectorReader.from

private const val BRANCH_IID_TYPE_ID: Byte = 1
private const val BRANCH_RECENCY_TYPE_ID: Byte = 2
private const val LEAF_TYPE_ID: Byte = 3

class ArrowHashTrie(private val nodesVec: DenseUnionVector) :
    HashTrie<ArrowHashTrie.Node> {

    private val iidBranchVec: ListVector = nodesVec.getVectorByType(BRANCH_IID_TYPE_ID) as ListVector
    private val iidBranchElVec: IntVector = iidBranchVec.dataVector as IntVector

    private val recencyBranchVec = from(nodesVec.getVectorByType(BRANCH_RECENCY_TYPE_ID))
    private val recencyVec = recencyBranchVec.mapKeyReader()
    private val recencyIdxVec = recencyBranchVec.mapValueReader()

    private val dataPageIdxVec: IntVector =
        (nodesVec.getVectorByType(LEAF_TYPE_ID) as StructVector)
            .getChild("data-page-idx", IntVector::class.java)

    interface Node : HashTrie.Node<Node>

    inner class IidBranch(override val path: ByteArray, branchVecIdx: Int) : Node {
        private val startIdx = iidBranchVec.getElementStartIndex(branchVecIdx)
        private val count = iidBranchVec.getElementEndIndex(branchVecIdx) - startIdx

        override val iidChildren: Array<Node?>
            get() = Array(count) { childBucket ->
                val childIdx = childBucket + startIdx
                if (iidBranchElVec.isNull(childIdx))
                    null
                else
                    forIndex(conjPath(path, childBucket.toByte()), iidBranchElVec[childIdx])
            }

        override val recencies = null
        override fun recencyNode(idx: Int) = throw UnsupportedOperationException()
    }

    inner class RecencyBranch(override val path: ByteArray, branchVecIdx: Int) : Node {

        override val iidChildren = null

        private val startIdx = recencyBranchVec.getListStartIndex(branchVecIdx)
        private val count = recencyBranchVec.getListCount(branchVecIdx)

        override val recencies: RecencyArray
            get() {
                return RecencyArray(count) { idx ->
                    recencyVec.getLong(idx + startIdx)
                }
            }

        override fun recencyNode(idx: Int) =
            checkNotNull(forIndex(path, recencyIdxVec.getInt(startIdx + idx)))

    }

    inner class Leaf(override val path: ByteArray, private val leafOffset: Int) : Node {
        val dataPageIndex: Int get() = dataPageIdxVec[leafOffset]

        override val iidChildren = null
        override val recencies = null
        override fun recencyNode(idx: Int) = throw UnsupportedOperationException()
    }

    private fun forIndex(path: ByteArray, idx: Int): Node? {
        val nodeOffset = nodesVec.getOffset(idx)

        return when (nodesVec.getTypeId(idx)) {
            0.toByte() -> null
            BRANCH_IID_TYPE_ID -> IidBranch(path, nodeOffset)
            BRANCH_RECENCY_TYPE_ID -> RecencyBranch(path, nodeOffset)
            LEAF_TYPE_ID -> Leaf(path, nodeOffset)
            else -> throw UnsupportedOperationException()
        }
    }

    override val rootNode get() = forIndex(ByteArray(0), nodesVec.valueCount - 1)
}

