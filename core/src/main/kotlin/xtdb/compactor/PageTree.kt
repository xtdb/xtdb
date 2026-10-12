package xtdb.compactor

import com.carrotsearch.hppc.ByteArrayList
import xtdb.trie.RecencyRange

private const val BRANCH_FACTOR = 4

sealed interface PageTree {
    val rowCount: Int
    val recency: RecencyRange

    data class Node(
        val children: MutableList<PageTree?> = MutableList(BRANCH_FACTOR) { null },
        override var rowCount: Int = 0,
        override var recency: RecencyRange = RecencyRange.EMPTY
    ) : PageTree {
        override fun toString() = "(Node [${children.joinToString()}], $rowCount)"

        override val leaves: List<Leaf> get() = children.flatMap { it?.leaves.orEmpty() }
    }

    /**
     * @param recency the range of the page's `_recency`; [RecencyRange.EMPTY] when the data carries none
     */
    data class Leaf(
        val pageIdx: Int, val part: ByteArrayList, override val rowCount: Int, override val recency: RecencyRange
    ) : PageTree {
        override fun toString() = "(Leaf #$pageIdx, $rowCount)"

        override val leaves: List<Leaf> get() = listOf(this)
    }

    val leaves: List<Leaf>

    companion object {
        val List<Leaf>.asTree: PageTree?
            get() {
                if (isEmpty()) return null
                singleOrNull()?.let { if (it.part.isEmpty) return it }

                val rootNode = Node()

                forEach { leaf ->
                    val part = leaf.part
                    val partLen = part.size()
                    val rowCount = leaf.rowCount

                    var node = rootNode

                    for (depth in 0..<partLen - 1) {
                        val partElem = part[depth].toInt()
                        node.rowCount += rowCount
                        node.recency += leaf.recency
                        node = node.children[partElem] as Node? ?: Node().also { node.children[partElem] = it }
                    }

                    node.rowCount += rowCount
                    node.recency += leaf.recency
                    node.children[part[partLen - 1].toInt()] = leaf
                }

                return rootNode
            }
    }
}
