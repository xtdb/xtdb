package xtdb.trie

import com.carrotsearch.hppc.ObjectStack
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.trie.ArrowHashTrie.IidBranch
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

interface HashTrie<N : Node<N>, L : N> {
    val rootNode: N?

    val leaves get() = rootNode?.leaves ?: emptyList()

    interface Node<N : Node<N>> {
        val path: ByteArray

        val hashChildren: Array<N?>?

        fun leafStream(): Stream<out Node<N>> =
            when {
                hashChildren != null -> Arrays.stream(hashChildren).flatMap { child -> child?.leafStream() }
                else -> Stream.of(this)
            }

        val leaves: List<Node<N>> get() = leafStream().toList()
    }

    @Suppress("MemberVisibilityCanBePrivate")
    companion object {

        const val LEVEL_BITS: Int = 2
        const val LEVEL_WIDTH: Int = 1 shl LEVEL_BITS
        const val LEVEL_MASK: Int = LEVEL_WIDTH - 1

        @JvmStatic
        fun bucketFor(byteArray: ByteArray, level: Int): Byte {
            assert(level * LEVEL_BITS < byteArray.size * java.lang.Byte.SIZE)
            val bitIdx = level * LEVEL_BITS
            val byteIdx = bitIdx / java.lang.Byte.SIZE
            val bitOffset = bitIdx % java.lang.Byte.SIZE

            val b = byteArray.get(byteIdx)
            return ((b.toInt() ushr ((java.lang.Byte.SIZE - LEVEL_BITS) - bitOffset)) and LEVEL_MASK).toByte()
        }

        @JvmStatic
        fun bucketFor(pointer: ArrowBufPointer, level: Int): Byte {
            assert(level * LEVEL_BITS < pointer.length * java.lang.Byte.SIZE)
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

interface ISegment<N : Node<N>, L : N> {
    val trie: HashTrie<N, L>
    val dataRel: DataRel<L>?

    class Segment<N : Node<N>, L : N>(
        override val trie: HashTrie<N, L>,
        override val dataRel: DataRel<L>
    ) : ISegment<N, L>
}

data class MergePlanNode<N : Node<N>, L : N>(val segment: ISegment<N, L>, val node: L) {
    companion object {
        @Suppress("UNCHECKED_CAST")
        fun <N : Node<N>, L : N> create(segment: ISegment<N, L>, node: Node<*>): MergePlanNode<N, L> =
            MergePlanNode(segment, node as L)
    }
}

class MergePlanTask(val mpNodes: List<MergePlanNode<*, *>>, val path: ByteArray)

// IMPORTANT - Tries (i.e. segments) and nodes need to be returned in system time order
@Suppress("UNUSED_EXPRESSION")
fun List<ISegment<*, *>>.toMergePlan(
    pathPred: Predicate<ByteArray>?,
): List<MergePlanTask> {
    val result = mutableListOf<MergePlanTask>()
    val stack = ObjectStack<MergePlanTask>()

    val initialMpNodes = mapNotNull { seg -> seg.trie.rootNode?.let { MergePlanNode.create(seg, it) } }
    if (initialMpNodes.isNotEmpty()) stack.push(MergePlanTask(initialMpNodes, ByteArray(0)))

    while (!stack.isEmpty) {
        val mergePlanTask = stack.pop()
        val mpNodes = mergePlanTask.mpNodes

        when {
            pathPred != null && !pathPred.test(mergePlanTask.path) -> null

            mpNodes.any { it.node is IidBranch || it.node is MemoryHashTrie.Branch } -> {
                val nodeChildren = mpNodes.map { it.node.hashChildren }
                // do these in reverse order so that they're on the stack in path-prefix order
                for (bucketIdx in HashTrie.LEVEL_WIDTH - 1 downTo 0) {
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