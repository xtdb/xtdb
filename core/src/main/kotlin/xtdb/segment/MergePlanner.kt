package xtdb.segment

import com.carrotsearch.hppc.ObjectStack
import xtdb.segment.Segment.PageMeta
import xtdb.trie.DEFAULT_LEVEL_WIDTH
import xtdb.trie.HashTrie
import xtdb.trie.conjPath
import xtdb.util.safeMap
import xtdb.util.useAll
import java.util.function.Predicate

object MergePlanner {
    private data class SegmentNode<L>(
        val segMeta: Segment.Metadata<L>, val node: HashTrie.Node<L>
    ) {
        val children: List<SegmentNode<L>?>?
            get() = node.hashChildren?.map { it?.let { SegmentNode(segMeta, it) } }

        @Suppress("UNCHECKED_CAST")
        val page get() = segMeta.page(node as L)
    }

    private fun <L> Segment.Metadata<L>.toSegmentNode() = trie.rootNode?.let { node -> SegmentNode(this, node) }

    private class WorkTask(val segNodes: List<SegmentNode<*>>, val path: ByteArray)

    @FunctionalInterface
    fun interface PagesFilter {
        fun filterPages(pages: List<PageMeta<*>>): List<PageMeta<*>>?
    }

    // IMPORTANT - Tries (i.e. segments) and nodes need to be returned in system time order
    @JvmStatic
    @JvmOverloads
    fun plan(
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
                val result = mutableListOf<MergeTask>()

                val initialSegNodes = segMetas.mapNotNull { it.toSegmentNode() }
                if (initialSegNodes.isEmpty()) return emptyList()

                val stack = ObjectStack<WorkTask>()

                stack.push(WorkTask(initialSegNodes, ByteArray(0)))

                while (!stack.isEmpty) {
                    val workTask = stack.pop()
                    val segNodes = workTask.segNodes
                    if (pathPred != null && !pathPred.test(workTask.path)) continue

                    val nodeChildren = segNodes.map { it to it.children }
                    if (nodeChildren.any { (_, children) -> children != null }) {
                        // do these in reverse order so that they're on the stack in path-prefix order
                        for (bucketIdx in (0..<DEFAULT_LEVEL_WIDTH).reversed()) {
                            val newSegNodes = nodeChildren.mapNotNull { (segNode, children) ->
                                // children == null iff this is a leaf
                                // children[bucketIdx] is null iff this is a branch but without any values in this bucket.
                                if (children != null) children[bucketIdx] else segNode
                            }

                            if (newSegNodes.isNotEmpty())
                                stack.push(WorkTask(newSegNodes, conjPath(workTask.path, bucketIdx.toByte())))
                        }
                    } else {
                        filterPages.filterPages(segNodes.map { it.page })
                            ?.takeIf { it.isNotEmpty() }
                            ?.let { filteredPages -> result += MergeTask(filteredPages.map { it.page }, workTask.path) }
                    }
                }

                return result
            }
    }
}
