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
    hashChildren?.let { it.stream().flatMap { child -> child?.leafStream() } } ?: Stream.of(this as L)

interface HashTrie<L> {
    val rootNode: Node<L>?

    val leaves get() = rootNode?.leaves ?: emptyList()

    interface Node<L> {
        val path: ByteArray

        val hashChildren: List<Node<L>?>?

        val leaves: List<L> get() = leafStream().toList()
    }
}
