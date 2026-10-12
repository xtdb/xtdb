package xtdb.trie

import xtdb.trie.HashTrie.Node
import java.util.stream.Stream

internal fun conjPath(path: ByteArray, idx: Byte): ByteArray {
    val currentPathLength = path.size
    val childPath = ByteArray(currentPathLength + 1)
    System.arraycopy(path, 0, childPath, 0, currentPathLength)
    childPath[currentPathLength] = idx
    return childPath
}


@Suppress("UNCHECKED_CAST")
private fun <L> Node<L>.leafStream(): Stream<L> =
    when (this) {
        is HashTrie.IidBranch -> hashChildren.stream().flatMap { child -> child?.leafStream() }
        is HashTrie.RecencyBranch -> buckets.stream().flatMap { bucket -> bucket.node.leafStream() }
        is HashTrie.Leaf -> Stream.of(this as L)
    }


interface HashTrie<L> {
    val rootNode: Node<L>?

    val leaves get() = rootNode?.leaves ?: emptyList()

    sealed interface Node<L> {
        val path: ByteArray

        val leaves: List<L> get() = leafStream().toList()
    }

    interface IidBranch<L> : Node<L> {
        val hashChildren: List<Node<L>?>
    }

    /**
     * Every bucket's node sits at this branch's own IID [path].
     */
    interface RecencyBranch<L> : Node<L> {
        /**
         * @property recency the inclusive upper bound of the recency of every row under [node]
         */
        class Bucket<L>(val recency: RecencyMicros, val node: Node<L>)

        /**
         * in ascending order of [Bucket.recency]
         */
        val buckets: List<Bucket<L>>
    }

    interface Leaf<L> : Node<L>
}
