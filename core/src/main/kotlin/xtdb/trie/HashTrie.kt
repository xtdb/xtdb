package xtdb.trie

import xtdb.trie.HashTrie.Node
import java.util.*
import java.util.stream.Stream

internal fun conjPath(path: ByteArray, idx: Byte): ByteArray {
    val currentPathLength = path.size
    val childPath = ByteArray(currentPathLength + 1)
    System.arraycopy(path, 0, childPath, 0, currentPathLength)
    childPath[currentPathLength] = idx
    return childPath
}


private fun <N : Node<N>> Node<N>.leafStream(): Stream<Node<N>> =
    hashChildren?.let { it.stream().flatMap { child -> child?.leafStream() } } ?: Stream.of(this)


interface HashTrie<N : Node<N>, L : N> {
    val rootNode: N?

    val leaves get() = rootNode?.leaves ?: emptyList()

    interface Node<N : Node<N>> {
        val path: ByteArray

        val hashChildren: List<N?>?

        val leaves: List<Node<N>> get() = leafStream().toList()
    }
}
