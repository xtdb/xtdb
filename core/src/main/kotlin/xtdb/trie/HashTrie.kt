package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer
import java.util.*
import java.util.stream.Stream

internal const val LEVEL_BITS: Int = 2
internal const val LEVEL_WIDTH: Int = 1 shl LEVEL_BITS
internal const val LEVEL_MASK: Int = LEVEL_WIDTH - 1

interface HashTrie<N : HashTrie.Node<N>> {
    val rootNode: N?

    val leaves get() = rootNode?.leaves ?: emptyList()

    interface Node<N : Node<N>> {
        val path: ByteArray

        val children: Array<N?>?

        fun leafStream(): Stream<out Node<N>> =
            if (children == null)
                Stream.of(this)
            else
                Arrays.stream(children).flatMap { child -> child?.leafStream() }

        val leaves: List<Node<N>> get() = leafStream().toList()
    }

    companion object {
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
