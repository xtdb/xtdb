package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorIndirection.Mask
import xtdb.arrow.VectorReader
import xtdb.operator.SelectionSpec
import xtdb.trie.Bucketer
import java.util.*

class MultiIidSelector(private val iids: SortedSet<ByteArray>) : SelectionSpec {

    private val haystackPtr = ArrowBufPointer()
    private val needlePtr = ArrowBufPointer()

    override fun select(readRelation: RelationReader, mask: Mask): Mask {
        val res = Mask.Builder(readRelation.rowCount)
        val iidReader = readRelation["_iid"]

        for (rowIdx in mask.toSelection()) {
            if (iidReader[rowIdx] in iids) {
                res.add(rowIdx)
            }
        }

        return res.build()
    }

    private val bucketer = Bucketer.DEFAULT

    private fun skipAhead(iidReader: VectorReader, startIdx: Int): Int {
        var left = startIdx
        var right = iidReader.valueCount

        while (left < right) {
            val mid = (left + right) ushr 1
            iidReader.getPointer(mid, haystackPtr)
            if (haystackPtr < needlePtr) {
                left = mid + 1
            } else {
                right = mid
            }
        }

        return left
    }

    fun select(allocator: BufferAllocator, readRelation: RelationReader, path: ByteArray): Mask {
        val iids = bucketer.filterIidsForPath(this.iids, path)

        val iidReader = readRelation["_iid"]
        val res = Mask.Builder(iidReader.valueCount)

        FixedSizeBinaryVector(allocator, false, 16).use { needlesVec ->
            for (iid in iids) needlesVec.writeBytes(iid)

            var haystackIdx = 0
            var needleIdx = 0
            val haystackCount = iidReader.valueCount
            val needleCount = needlesVec.valueCount

            val binarySearchCost = if (haystackCount > 0) 31 - haystackCount.countLeadingZeroBits() else 0

            while (needleIdx < needleCount) {
                needlesVec.getPointer(needleIdx, needlePtr)

                while (haystackIdx < haystackCount) {
                    iidReader.getPointer(haystackIdx, haystackPtr)

                    val cmp = haystackPtr.compareTo(needlePtr)
                    when {
                        cmp > 0 -> break
                        cmp == 0 -> {
                            res.add(haystackIdx)
                            haystackIdx++
                        }
                        else -> {
                            val remainingNeedles = needleCount - needleIdx
                            val remainingHaystack = haystackCount - haystackIdx
                            // Binary search is worth it if expected linear scans exceed the log cost
                            if (remainingHaystack / remainingNeedles > binarySearchCost) {
                                haystackIdx = skipAhead(iidReader, haystackIdx + 1)
                            } else {
                                haystackIdx++
                            }
                        }
                    }
                }

                needleIdx++
            }
        }

        return res.build()
    }

    override fun close() = Unit
}