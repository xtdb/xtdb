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
}
