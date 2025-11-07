package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.IntVector
import xtdb.arrow.VectorReader
import xtdb.util.closeOnCatch
import java.util.function.IntUnaryOperator

private const val DEFAULT_LOAD_FACTOR = 0.6

class BuildSideMap private constructor(
    private val srcIdxs: IntVector,
    private val srcHashes: IntVector,
    private val hashMask: Int,
) : AutoCloseable {

    var tombstones: RoaringBitmap? = null

    fun findValue(hash: Int, cmp: IntUnaryOperator, removeOnMatch: Boolean): Int {
        var lookupIdx = hash and hashMask

        while (true) {
            if (srcIdxs.isNull(lookupIdx)) return -1
            val idx = srcIdxs.getInt(lookupIdx)
            if ((tombstones == null || !tombstones!!.contains(idx)) && srcHashes.getInt(lookupIdx) == hash && cmp.applyAsInt(idx) == 1) {
                if (removeOnMatch) {
                    if (tombstones == null) tombstones = RoaringBitmap()
                    tombstones!!.add(idx)
                }
                return idx
            }
            lookupIdx = lookupIdx.inc() and hashMask
        }
    }

    fun iterator(hash: Int) = object : IntIterator() {
        private var lookupIdx = hash and hashMask
        private var nextValue = -1
        private var hasNextValue = false
        
        override fun hasNext(): Boolean {
            if (hasNextValue) return true

            while (true) {
                if (srcIdxs.isNull(lookupIdx)) return false
                if (srcHashes.getInt(lookupIdx) == hash) {
                    nextValue = srcIdxs.getInt(lookupIdx)
                    hasNextValue = true
                    lookupIdx = lookupIdx.inc() and hashMask
                    return true
                }
                lookupIdx = lookupIdx.inc() and hashMask
            }
        }

        override fun nextInt(): Int {
            if (!hasNext()) throw NoSuchElementException()
            hasNextValue = false
            return nextValue
        }
    }

    override fun close() {
        srcHashes.close()
        srcIdxs.close()
    }

    companion object {
        internal fun hashBits(rowCount: Int, loadFactor: Double = DEFAULT_LOAD_FACTOR) =
            Long.SIZE_BITS - (rowCount / loadFactor).toLong().countLeadingZeroBits()

        internal fun IntVector.insertionIdx(maskedHash: Int, hashMask: Int, skipIndex: IntArray): Int {
            var insertionIdx = skipIndex[maskedHash]

            while (true) {
                if (isNull(insertionIdx)) {
                    skipIndex[maskedHash] = skipIndex[insertionIdx.inc() and hashMask]
                    return insertionIdx
                }
                val newIdx = insertionIdx.inc() and hashMask
                skipIndex[insertionIdx] = skipIndex[newIdx]
                insertionIdx = skipIndex[newIdx]
            }
        }

        @JvmStatic
        @JvmOverloads
        fun from(al: BufferAllocator, hashCol: VectorReader, loadFactor: Double = DEFAULT_LOAD_FACTOR): BuildSideMap {

            val rowCount = hashCol.valueCount

            val hashBits = hashBits(rowCount, loadFactor)
            val mapSize = 1 shl hashBits
            val hashMask = mapSize - 1
            val skipIndex = IntArray(mapSize) { it }

            return IntVector.open(al, "src-idxs", true, mapSize).closeOnCatch { srcIdxs ->
                IntVector.open(al, "src-hashes", true, mapSize).closeOnCatch { srcHashes ->
                    repeat(rowCount) { idx ->
                        val hash = hashCol.getInt(idx)
                        val insertionIdx = srcIdxs.insertionIdx(hash and hashMask , hashMask, skipIndex)

                        srcIdxs[insertionIdx] = idx
                        srcHashes[insertionIdx] = hash
                    }
                    BuildSideMap(srcIdxs, srcHashes, hashMask)
                }
            }
        }
    }
}
