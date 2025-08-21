package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer


const val DEFAULT_LEVEL_BITS = 2
const val DEFAULT_LEVEL_WIDTH = 1 shl DEFAULT_LEVEL_BITS

data class Bucketer(val levelBits: Int = DEFAULT_LEVEL_BITS) {

    val levelWidth: Int = 1 shl levelBits
    val levelMask: Int = levelWidth - 1

    init {
        require(levelBits in setOf(2,4,8), {"levelBits must be one of 2, 4 or 8, got $levelBits"})
    }

    fun bucketFor(byteArray: ByteArray, level: Int): Byte {
        assert(level * levelBits < byteArray.size * java.lang.Byte.SIZE)
        val bitIdx = level * levelBits
        val byteIdx = bitIdx / java.lang.Byte.SIZE
        val bitOffset = bitIdx % java.lang.Byte.SIZE

        val b = byteArray.get(byteIdx)
        return ((b.toInt() ushr ((java.lang.Byte.SIZE - levelBits) - bitOffset)) and levelMask).toByte()
    }

    fun bucketFor(pointer: ArrowBufPointer, level: Int): Byte {
        assert(level * levelBits < pointer.length * java.lang.Byte.SIZE)
        val bitIdx = level * levelBits
        val byteIdx = bitIdx / java.lang.Byte.SIZE
        val bitOffset = bitIdx % java.lang.Byte.SIZE

        val b = pointer.buf!!.getByte(pointer.offset + byteIdx)
        return ((b.toInt() ushr ((java.lang.Byte.SIZE - levelBits) - bitOffset)) and levelMask).toByte()
    }

    fun compareToPath(pointer: ArrowBufPointer, path: ByteArray): Int {
        for (level in path.indices) {
            val cmp = bucketFor(pointer, level).toInt() compareTo (path[level].toInt())
            if (cmp != 0) return cmp
        }
        return 0
    }

    companion object {
        @JvmField
        val DEFAULT = Bucketer()
    }
}