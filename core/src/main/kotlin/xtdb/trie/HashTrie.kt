package xtdb.trie

import com.carrotsearch.hppc.ObjectStack
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Schema
import org.roaringbitmap.buffer.MutableRoaringBitmap
import xtdb.metadata.ITableMetadata
import xtdb.trie.ArrowHashTrie.IidBranch
import xtdb.trie.ArrowHashTrie.RecencyBranch
import xtdb.trie.HashTrie.Node
import xtdb.vector.RelationReader
import java.util.*
import java.util.function.IntPredicate
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

            val b = pointer.buf.getByte(pointer.offset + byteIdx)
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

interface IDataRel {
    fun getSchema(): Schema
    fun loadPage(leaf: Node<*>): RelationReader
}

interface Segment {
    val trie: HashTrie<*>
}

data class ArrowSegment(
    val tableMetadata: ITableMetadata, val pageIdxPred: IntPredicate, val trieKey: String,
    override val trie: HashTrie<ArrowHashTrie.Node>,
) : Segment

data class LiveSegment(val liveRel: RelationReader, override val trie: HashTrie<LiveHashTrie.Node>) : Segment

data class CompactorSegment(val dataRel: IDataRel, override val trie: HashTrie<ArrowHashTrie.Node>) : Segment

data class MergePlanNode(val segment: Segment, val node: Node<*>)

class MergePlanTask(val mpNodes: List<MergePlanNode>, val path: ByteArray)

@Suppress("UNUSED_EXPRESSION")
fun toMergePlan(segments: List<Segment>, pathPred: Predicate<ByteArray>?): List<MergePlanTask> {
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

            mpNodes.any { it.node is IidBranch } -> {
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

sealed class Leaf()

data class ArrowLeaf(val segment: ArrowSegment, val pageIdx: Int) : Leaf()

data class LiveLeaf(val liveRel: RelationReader) : Leaf()

class MergeTask(val leaves: List<Leaf>, val path: ByteArray)

private fun mergePlanTaskToMergeTask(mergePlanTask: MergePlanTask): MergeTask? {
    var nodeTaken = false
    val cumulativeIidBitmap = MutableRoaringBitmap()
    val leaves = mutableListOf<Leaf>()

    for (mpNode in mergePlanTask.mpNodes) when (mpNode.segment) {

        is ArrowSegment -> {
            val pageIdxPred = mpNode.segment.pageIdxPred
            val tableMetadata = mpNode.segment.tableMetadata
            val node = mpNode.node as ArrowHashTrie.Leaf
            val pageIdx = node.dataPageIndex
            val takeNode = pageIdxPred.test(pageIdx)
            if (takeNode) {
                val iidBloomBitmap = tableMetadata.iidBloomBitmap(pageIdx)
                if (iidBloomBitmap != null) cumulativeIidBitmap.or(iidBloomBitmap)
                leaves += ArrowLeaf(mpNode.segment, pageIdx)
            } else if (nodeTaken) {
                val iidBloomBitmap = tableMetadata.iidBloomBitmap(pageIdx)
                if (iidBloomBitmap != null && MutableRoaringBitmap.intersects(iidBloomBitmap, cumulativeIidBitmap))
                    leaves += ArrowLeaf(mpNode.segment, pageIdx)
            }

            nodeTaken = (nodeTaken or takeNode)
        }

        is LiveSegment -> {
            val liveRel = mpNode.segment.liveRel
            val trie = mpNode.segment.trie as LiveHashTrie
            val node = mpNode.node as LiveHashTrie.Leaf
            leaves += LiveLeaf(liveRel.select(node.mergeSort(trie)))
            nodeTaken = true
        }
    }

    if (nodeTaken) return MergeTask(leaves, mergePlanTask.path)
    return null
}

fun toMergeTasks(segments: List<Segment>, pathPred: Predicate<ByteArray>?): List<MergeTask> {
    return toMergePlan(segments, pathPred).mapNotNull(::mergePlanTaskToMergeTask)
}
