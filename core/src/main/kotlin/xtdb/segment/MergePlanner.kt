package xtdb.segment

import com.carrotsearch.hppc.ObjectStack
import kotlinx.coroutines.runBlocking
import xtdb.segment.Segment.PageMeta
import xtdb.trie.DEFAULT_LEVEL_WIDTH
import xtdb.trie.HashTrie
import xtdb.trie.conjPath
import xtdb.util.safeMap
import xtdb.util.useAll
import java.util.Collections
import java.util.IdentityHashMap
import java.util.function.Predicate

object MergePlanner {
    private sealed interface SegmentNode<L> {
        val segMeta: Segment.Metadata<L>

        class Branch<L>(override val segMeta: Segment.Metadata<L>, val node: HashTrie.IidBranch<L>) : SegmentNode<L> {
            val children: List<List<SegmentNode<L>>>
                get() = node.hashChildren.map { child -> child?.let { segNodes(segMeta, it) }.orEmpty() }
        }

        class Leaf<L>(override val segMeta: Segment.Metadata<L>, val node: HashTrie.Leaf<L>) : SegmentNode<L> {
            @Suppress("UNCHECKED_CAST")
            val page get() = segMeta.page(node as L)
        }
    }

    private fun <L> segNodes(segMeta: Segment.Metadata<L>, node: HashTrie.Node<L>): List<SegmentNode<L>> =
        when (node) {
            is HashTrie.IidBranch -> listOf(SegmentNode.Branch(segMeta, node))
            is HashTrie.RecencyBranch -> node.buckets.flatMap { segNodes(segMeta, it.node) }
            is HashTrie.Leaf -> listOf(SegmentNode.Leaf(segMeta, node))
        }

    private fun <L> Segment.Metadata<L>.toSegmentNodes() = trie.rootNode?.let { segNodes(this, it) }.orEmpty()

    private class WorkTask(val segNodes: List<SegmentNode<*>>, val path: ByteArray)

    @FunctionalInterface
    fun interface PagesFilter {
        fun filterPages(pages: List<PageMeta<*>>): List<PageMeta<*>>?
    }

    @JvmStatic
    fun planSync(
        segments: List<Segment<*>>,
        pathPred: Predicate<ByteArray>?,
        filterPages: PagesFilter = PagesFilter { it }
    ) = runBlocking { plan(segments, pathPred, filterPages) }

    suspend fun plan(
        segments: List<Segment<*>>,
        pathPred: Predicate<ByteArray>?,
        filterPages: PagesFilter = PagesFilter { it }
    ): List<MergeTask> {
        segments
            .filter {
                val part = it.part
                if (part == null || pathPred == null) true
                else pathPred.test(part)
            }
            .safeMap { it.openMetadata() }.useAll { segMetas ->
                val result = mutableListOf<Pair<List<Segment.Page<*>>, ByteArray>>()

                val initialSegNodes = segMetas.flatMap { it.toSegmentNodes() }
                if (initialSegNodes.isEmpty()) return emptyList()

                val stack = ObjectStack<WorkTask>()

                stack.push(WorkTask(initialSegNodes, ByteArray(0)))

                while (!stack.isEmpty) {
                    val workTask = stack.pop()
                    val segNodes = workTask.segNodes
                    if (pathPred != null && !pathPred.test(workTask.path)) continue

                    val leaves = segNodes.filterIsInstance<SegmentNode.Leaf<*>>()
                    if (leaves.size < segNodes.size) {
                        val nodeChildren = segNodes.map { segNode ->
                            when (segNode) {
                                is SegmentNode.Branch -> segNode.children
                                is SegmentNode.Leaf -> null
                            }
                        }

                        // do these in reverse order so that they're on the stack in path-prefix order
                        for (bucketIdx in (0..<DEFAULT_LEVEL_WIDTH).reversed()) {
                            val newSegNodes = segNodes.zip(nodeChildren).flatMap { (segNode, children) ->
                                // a leaf covers every bucket below it
                                children?.get(bucketIdx) ?: listOf(segNode)
                            }

                            if (newSegNodes.isNotEmpty())
                                stack.push(WorkTask(newSegNodes, conjPath(workTask.path, bucketIdx.toByte())))
                        }
                    } else {
                        filterPages.filterPages(leaves.map { it.page })
                            ?.takeIf { it.isNotEmpty() }
                            ?.let { filteredPages -> result += filteredPages.map { it.page } to workTask.path }
                    }
                }

                val laterPages = Collections.newSetFromMap(IdentityHashMap<Segment.Page<*>, Boolean>())

                return result.asReversed()
                    .map { (pages, path) -> MergeTask(pages, path, lastUsedPages = pages.filter { laterPages.add(it) }) }
                    .asReversed()
            }
    }
}
