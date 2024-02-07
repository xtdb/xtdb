package xtdb.trie

import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.StructVector

private const val BRANCH_TYPE_ID: Byte = 1
private const val LEAF_TYPE_ID: Byte = 2

class ArrowHashTrie(private val nodesVec: DenseUnionVector) :
    HashTrie<ArrowHashTrie.Node> {

    private val branchVec: ListVector = nodesVec.getVectorByType(BRANCH_TYPE_ID) as ListVector
    private val branchElVec: IntVector = branchVec.dataVector as IntVector
    private val dataPageIdxVec: IntVector =
        (nodesVec.getVectorByType(LEAF_TYPE_ID) as StructVector)
            .getChild("data-page-idx", IntVector::class.java)

    interface Node : HashTrie.Node<Node>

    inner class Branch(override val path: ByteArray, private val branchVecIdx: Int) : Node {
        override val children: Array<Node?>
            get() {
                val startIdx = branchVec.getElementStartIndex(branchVecIdx)

                return Array(branchVec.getElementEndIndex(branchVecIdx) - startIdx) { childBucket ->
                    val childIdx = childBucket + startIdx
                    if (branchElVec.isNull(childIdx))
                        null
                    else
                        forIndex(conjPath(path, childBucket.toByte()), branchElVec[childIdx])
                }
            }
    }

    inner class Leaf(override val path: ByteArray, private val leafOffset: Int) : Node {
        val dataPageIndex: Int get() = dataPageIdxVec[leafOffset]

        override val children = null
    }

    private fun forIndex(path: ByteArray, idx: Int): Node? {
        val nodeOffset = nodesVec.getOffset(idx)

        return when (nodesVec.getTypeId(idx)) {
            0.toByte() -> null
            BRANCH_TYPE_ID -> Branch(path, nodeOffset)
            LEAF_TYPE_ID -> Leaf(path, nodeOffset)
            else -> throw UnsupportedOperationException()
        }
    }

    override val rootNode get() = forIndex(ByteArray(0), nodesVec.valueCount - 1)

    companion object {
        private fun conjPath(path: ByteArray, idx: Byte): ByteArray {
            val currentPathLength = path.size
            val childPath = ByteArray(currentPathLength + 1)
            System.arraycopy(path, 0, childPath, 0, currentPathLength)
            childPath[currentPathLength] = idx
            return childPath
        }
    }
}
