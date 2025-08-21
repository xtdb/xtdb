@file:JvmName("MergePlan")
package xtdb.trie

import com.carrotsearch.hppc.ObjectStack
import java.util.function.Predicate

data class MergePlanNode<N : HashTrie.Node<N>, L : N>(val segment: ISegment<N, L>, val node: L) {
    companion object {
        @Suppress("UNCHECKED_CAST")
        fun <N : HashTrie.Node<N>, L : N> create(segment: ISegment<N, L>, node: HashTrie.Node<*>): MergePlanNode<N, L> =
            MergePlanNode(segment, node as L)
    }
}

class MergePlanTask(val mpNodes: List<MergePlanNode<*, *>>, val path: ByteArray)

// IMPORTANT - Tries (i.e. segments) and nodes need to be returned in system time order
fun List<ISegment<*, *>>.toMergePlan(pathPred: Predicate<ByteArray>?): List<MergePlanTask> {
    val result = mutableListOf<MergePlanTask>()
    val stack = ObjectStack<MergePlanTask>()

    val initialMpNodes = mapNotNull { seg -> seg.trie.rootNode?.let { MergePlanNode.create(seg, it) } }
    if (initialMpNodes.isNotEmpty()) stack.push(MergePlanTask(initialMpNodes, ByteArray(0)))

    while (!stack.isEmpty) {
        val mergePlanTask = stack.pop()
        val mpNodes = mergePlanTask.mpNodes

        when {
            pathPred != null && !pathPred.test(mergePlanTask.path) -> null

            mpNodes.any { it.node is ArrowHashTrie.IidBranch || it.node is MemoryHashTrie.Branch } -> {
                val nodeChildren = mpNodes.map { it.node.hashChildren }
                // do these in reverse order so that they're on the stack in path-prefix order
                for (bucketIdx in DEFAULT_LEVEL_WIDTH - 1 downTo 0) {
                    val newMpNodes = nodeChildren.mapIndexedNotNull { idx, children ->
                        if (children != null) {
                            children[bucketIdx]?.let { MergePlanNode.create(mpNodes[idx].segment, it) }
                        } else {
                            mpNodes[idx]
                        }
                    }

                    if (newMpNodes.isNotEmpty())
                        stack.push(MergePlanTask(newMpNodes, conjPath(mergePlanTask.path, bucketIdx.toByte())))
                }
            }

            else -> result += mergePlanTask
        }
    }
    return result
}