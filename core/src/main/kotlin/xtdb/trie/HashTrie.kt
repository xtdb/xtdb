package xtdb.trie

import com.carrotsearch.hppc.ObjectStack
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.trie.ArrowHashTrie.IidBranch
import xtdb.trie.ArrowHashTrie.RecencyBranch
import xtdb.trie.HashTrie.Node
import java.util.*
import java.util.function.Predicate
import java.util.stream.Stream

internal typealias RecencyArray = LongArray

fun conjPath(path: ByteArray, idx: Byte): ByteArray {
    val currentPathLength = path.size
    val childPath = ByteArray(currentPathLength + 1)
    System.arraycopy(path, 0, childPath, 0, currentPathLength)
    childPath[currentPathLength] = idx
    return childPath
}

interface HashTrie<N : Node<N>> {
    val rootNode: N?

    val leaves get() = rootNode?.leaves ?: emptyList()

    interface Node<N : Node<N>> {
        val path: ByteArray

        val iidChildren: Array<N?>?

        val recencies: RecencyArray?
        fun recencyNode(idx: Int): N

        fun leafStream(): Stream<out Node<N>> =
            if (iidChildren == null)
                Stream.of(this)
            else
                Arrays.stream(iidChildren).flatMap { child -> child?.leafStream() }

        val leaves: List<Node<N>> get() = leafStream().toList()
    }

    abstract class BaseNode : Node<BaseNode>

    @Suppress("MemberVisibilityCanBePrivate")
    companion object {

        const val LEVEL_BITS: Int = 2
        const val LEVEL_WIDTH: Int = 1 shl LEVEL_BITS
        const val LEVEL_MASK: Int = LEVEL_WIDTH - 1

        @JvmStatic
        fun bucketFor(pointer: ArrowBufPointer, level: Int): Byte {
            val bitIdx = level * LEVEL_BITS
            val byteIdx = bitIdx / java.lang.Byte.SIZE
            val bitOffset = bitIdx % java.lang.Byte.SIZE

            val b = pointer.buf!!.getByte(pointer.offset + byteIdx)
            return ((b.toInt() ushr ((java.lang.Byte.SIZE - LEVEL_BITS) - bitOffset)) and LEVEL_MASK).toByte()
        }

        @JvmStatic
        fun compareToPath(pointer: ArrowBufPointer, path: ByteArray): Int {
            for (level in path.indices) {
                val cmp = bucketFor(pointer, level).toInt() compareTo (path[level].toInt())
                if (cmp != 0) return cmp
            }

            return 0
        }
    }
}

interface ISegment {
    val trie: HashTrie<*>
}

data class MergePlanNode(val segment: ISegment, val node: Node<*>)

class MergePlanTask(val mpNodes: List<MergePlanNode>, val path: ByteArray)

@Suppress("UNUSED_EXPRESSION")
fun toMergePlan(segments: List<ISegment>, pathPred: Predicate<ByteArray>?): List<MergePlanTask> {
    val result = mutableListOf<MergePlanTask>()
    val stack = ObjectStack<MergePlanTask>()

    val initialMpNodes = segments.mapNotNull { seg -> seg.trie.rootNode?.let { MergePlanNode(seg, it) } }
    if (initialMpNodes.isNotEmpty()) stack.push(MergePlanTask(initialMpNodes, ByteArray(0)))

    while (!stack.isEmpty) {
        val mergePlanTask = stack.pop()
        val mpNodes = mergePlanTask.mpNodes

        when {
            mpNodes.any { it.node is RecencyBranch } -> {
                val newMpNodes = mutableListOf<MergePlanNode>()
                for (mpNode in mergePlanTask.mpNodes) {
                    // TODO filter by recencies #3166
                    val recencies = mpNode.node.recencies
                    if (recencies != null) {
                        for (i in recencies.indices) {
                            newMpNodes += MergePlanNode(mpNode.segment, mpNode.node.recencyNode(i))
                        }
                    } else {
                        newMpNodes += mpNode
                    }
                }
                stack.push(MergePlanTask(newMpNodes, mergePlanTask.path))
            }

            pathPred != null && !pathPred.test(mergePlanTask.path) -> null

            mpNodes.any { it.node is IidBranch || it.node is LiveHashTrie.Branch } -> {
                val nodeChildren = mpNodes.map { it.node.iidChildren }
                // do these in reverse order so that they're on the stack in path-prefix order
                for (bucketIdx in HashTrie.LEVEL_WIDTH - 1 downTo 0) {
                    val newMpNodes = nodeChildren.mapIndexedNotNull { idx, children ->
                        if (children != null) {
                            children[bucketIdx]?.let { MergePlanNode(mpNodes[idx].segment, it) }
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