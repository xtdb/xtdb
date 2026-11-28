package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.LongLongVectorReader
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorIndirection.Mask
import xtdb.operator.SelectionSpec
import xtdb.trie.Bucketer
import xtdb.util.requireIid
import java.util.*

class MultiIidSelector(private val iids: SortedSet<ByteArray>) : SelectionSpec {

    init {
        iids.forEach(::requireIid)
    }

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

    private fun skipAhead(iidReader: LongLongVectorReader, startIdx: Int, needleHigh: Long, needleLow: Long): Int {
        var left = startIdx
        var right = iidReader.valueCount

        while (left < right) {
            val mid = (left + right) ushr 1
            if (iidReader.compareAt(mid, needleHigh, needleLow) < 0) {
                left = mid + 1
            } else {
                right = mid
            }
        }

        return left
    }

    fun select(allocator: BufferAllocator, readRelation: RelationReader, path: ByteArray): Mask {
        val iids = bucketer.filterIidsForPath(this.iids, path)

        val iidReader = readRelation["_iid"] as LongLongVectorReader
        val res = Mask.Builder(iidReader.valueCount)

        FixedSizeBinaryVector(allocator, false, 16).use { needlesVec ->
            for (iid in iids) needlesVec.writeBytes(iid)

            var haystackIdx = 0
            var needleIdx = 0
            val haystackCount = iidReader.valueCount
            val needleCount = needlesVec.valueCount

            val binarySearchCost = if (haystackCount > 0) 31 - haystackCount.countLeadingZeroBits() else 0

            while (needleIdx < needleCount) {
                val needleHigh = needlesVec.getLongLongHigh(needleIdx)
                val needleLow = needlesVec.getLongLongLow(needleIdx)

                while (haystackIdx < haystackCount) {
                    val cmp = iidReader.compareAt(haystackIdx, needleHigh, needleLow)
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
                                haystackIdx = skipAhead(iidReader, haystackIdx + 1, needleHigh, needleLow)
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