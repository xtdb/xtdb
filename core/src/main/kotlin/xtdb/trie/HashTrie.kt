package xtdb.trie

import clojure.lang.Keyword
import com.carrotsearch.hppc.ObjectStack
import org.apache.arrow.memory.util.ArrowBufPointer
import org.roaringbitmap.buffer.MutableRoaringBitmap
import xtdb.metadata.ITableMetadata
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
data class MergePlanNode(val segment: Map<*,*>, val node: Node<*>?)
data class MergePlanTask(val mpNodes: List<MergePlanNode>, val path: ByteArray)

private val trieKey = Keyword.intern("trie")
@Suppress("UNUSED_EXPRESSION")
fun toMergePlan(segments: List<Map<Keyword, Any>?>, pathPred: Predicate<ByteArray>?) : ArrayList<MergePlanTask> {
    val result: ArrayList<MergePlanTask> = ArrayList()
    val stack : ObjectStack<MergePlanTask> = ObjectStack()

    val initialMpNodes = ArrayList<MergePlanNode>()
    for (segment in segments){
        val trie = segment?.get(trieKey) as HashTrie<*>?
        if (trie != null) segment?.let { MergePlanNode(it, trie.rootNode) }?.let { initialMpNodes.add(it) }
    }

    if (initialMpNodes.size > 0) stack.push(MergePlanTask(initialMpNodes, ByteArray(0)))

    while (!stack.isEmpty) {
        val mergePlanTask = stack.pop()
        val mpNodes = mergePlanTask.mpNodes
        when {
            mpNodes.any { it.node is ArrowHashTrie.RecencyBranch} -> {
                val newMpNodes = ArrayList<MergePlanNode>()
                for (mpNode in mergePlanTask.mpNodes){
                    // TODO filter by recencies #3166
                    if (mpNode.node?.recencies != null) {
                        for(i in 0 until mpNode.node.recencies!!.size) {
                            newMpNodes.add(MergePlanNode(mpNode.segment, mpNode.node.recencyNode(i)))
                        }
                    } else {
                        newMpNodes.add(mpNode)
                    }
                }
                stack.push(MergePlanTask(newMpNodes, mergePlanTask.path))
            }
            pathPred?.test(mergePlanTask.path)?.not() == true ->  null
            mpNodes.any { it.node is ArrowHashTrie.IidBranch } -> {
                val nodeChildren = mpNodes.map { it.node?.iidChildren }
                // do these in reverse order so that they're on the stack in path-prefix order
                for (bucketIdx in HashTrie.LEVEL_WIDTH - 1 downTo 0) {
                    val newMpNodes = ArrayList<MergePlanNode>()
                    for (i in nodeChildren.indices) {
                        val children : Array<out Node<*>?>? = nodeChildren.get(i)
                        if (children != null) {
                            children.get(bucketIdx)?.also { newMpNodes.add(MergePlanNode(mpNodes.get(i).segment, it)) }
                        } else {
                            newMpNodes.add(mpNodes.get(i))
                        }
                    }
                    if (newMpNodes.size > 0) stack.push(MergePlanTask(newMpNodes, conjPath(mergePlanTask.path, bucketIdx.toByte())))
                }
            }
            else ->  result.add(mergePlanTask)
        }
    }
    return result
}

abstract class Leaf()
data class ArrowLeaf(val segment: Any, val pageIdx: Int) : Leaf()
data class LiveLeaf(val liveRel: RelationReader) : Leaf()

data class MergeTask(val leaves: List<Leaf>, val path: ByteArray)

private val pageIdxPredKey = Keyword.intern("page-idx-pred")
private val tableMetadataKey = Keyword.intern("table-metadata")
private val liveRelKey= Keyword.intern("live-rel")

fun toMergeTasks(segments: List<Map<Keyword, Any>?>, pathPred: Predicate<ByteArray>?) : ArrayList<Any> {
    val result = ArrayList<Any>()
    for (mergePlanTask in toMergePlan(segments, pathPred)) {
        var nodeTaken = false
        val cumulativeIidBitmap = MutableRoaringBitmap()
        val leaves = ArrayList<Leaf>()
        for(mpNode in mergePlanTask.mpNodes) when (mpNode.node) {
            is ArrowHashTrie.Leaf -> {
                val pageIdxPred = mpNode.segment.get(pageIdxPredKey) as IntPredicate
                val tableMetadata = mpNode.segment.get(tableMetadataKey) as ITableMetadata
                val pageIdx = mpNode.node.dataPageIndex
                val takeNode = pageIdxPred.test(pageIdx)
                if (takeNode) {
                    val iidBloomBitmap = tableMetadata.iidBloomBitmap(pageIdx)
                    cumulativeIidBitmap.or(iidBloomBitmap)
                    leaves.add(ArrowLeaf(mpNode.segment, pageIdx))
                } else if (nodeTaken && tableMetadata.iidBloomBitmap(pageIdx)?.let { MutableRoaringBitmap.intersects( it, cumulativeIidBitmap ) }!!) {
                    leaves.add(ArrowLeaf(mpNode.segment, pageIdx))
                }

                (nodeTaken or takeNode).also { nodeTaken = it }
            }
            is LiveHashTrie.Leaf -> {
                val liveRel = mpNode.segment.get(liveRelKey) as RelationReader
                val trie = mpNode.segment.get(trieKey) as LiveHashTrie
                leaves.add(LiveLeaf(liveRel.select(mpNode.node.mergeSort(trie))))
                nodeTaken = true
            }
        }
        if (nodeTaken) result.add(MergeTask(leaves, mergePlanTask.path))
    }
    return result
}
