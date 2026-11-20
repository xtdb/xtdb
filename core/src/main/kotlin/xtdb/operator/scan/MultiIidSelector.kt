package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.RelationReader
import xtdb.arrow.RowMask
import xtdb.arrow.VectorReader
import xtdb.trie.Bucketer
import java.util.*

class MultiIidSelector(private val iids: SortedSet<ByteArray>) : IidSelector {

    private val haystackPtr = ArrowBufPointer()
    private val needlePtr = ArrowBufPointer()

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

    override fun rowMask(al: BufferAllocator, page: RelationReader, path: ByteArray): RowMask {
        val iids = bucketer.filterIidsForPath(this.iids, path)

        val iidReader = page["_iid"]
        val mask = RowMask.Builder(iidReader.valueCount)

        FixedSizeBinaryVector(al, "_iid", false, 16).use { needlesVec ->
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
                            mask.add(haystackIdx)
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

        return mask.build()
    }
}