package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer


const val DEFAULT_LEVEL_BITS = 2
const val DEFAULT_LEVEL_WIDTH = 1 shl DEFAULT_LEVEL_BITS

data class Bucketer(val levelBits: Int = DEFAULT_LEVEL_BITS) {

    val levelWidth: Int = 1 shl levelBits
    val levelMask: Int = levelWidth - 1

    init {
        require(levelBits in setOf(2, 4, 8), { "levelBits must be one of 2, 4 or 8, got $levelBits" })
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

    fun compareToPath(bytes: ByteArray, path: ByteArray): Int {
        for (level in path.indices) {
            val cmp = bucketFor(bytes, level).toInt() compareTo (path[level].toInt())
            if (cmp != 0) return cmp
        }
        return 0
    }

    fun startIid(path: ByteArray): ByteArray {
        val iid = ByteArray(16)

        for (level in path.indices) {
            val bucket = path[level]
            val bitIdx = level * levelBits
            val byteIdx = bitIdx / java.lang.Byte.SIZE
            val bitOffset = bitIdx % java.lang.Byte.SIZE

            iid[byteIdx] =
                iid[byteIdx].toInt()
                    .or(bucket.toInt().shl(Byte.SIZE_BITS - levelBits - bitOffset))
                    .toByte()
        }

        return iid
    }

    fun incrementPath(path: ByteArray): ByteArray? {
        val nextPath = path.copyOf()

        for (i in nextPath.indices.reversed()) {
            val incremented = (nextPath[i].toInt() and levelMask) + 1
            if (incremented < levelWidth) {
                nextPath[i] = incremented.toByte()
                return nextPath
            }

            // Carry over: set this level to 0 and continue
            nextPath[i] = 0
        }

        return null
    }

    companion object {
        @JvmField
        val DEFAULT = Bucketer()
    }
}