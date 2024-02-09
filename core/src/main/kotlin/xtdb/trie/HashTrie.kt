package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer
import java.util.*
import java.util.stream.Stream

internal typealias RecencyArray = LongArray

interface HashTrie<N : HashTrie.Node<N>> {
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
