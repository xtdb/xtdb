package xtdb.operator.scan

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.operator.SelectionSpec
import xtdb.trie.Bucketer
import java.util.*

class MultiIidSelector(private val iids: SortedSet<ByteArray>) : SelectionSpec {

    private val haystackPtr = ArrowBufPointer()
    private val needlePtr = ArrowBufPointer()

    private fun select(readRelation: RelationReader, iids: SortedSet<ByteArray>): IntArray {
        val res = IntArrayList()
        val iidReader = readRelation["_iid"]

        repeat(readRelation.rowCount) { rowIdx ->
            if (iidReader[rowIdx] in iids) {
                res.add(rowIdx)
            }
        }

        return res.toArray()
    }

    override fun select(
        allocator: BufferAllocator, readRelation: RelationReader, schema: Map<String, Any>, params: RelationReader
    ) = select(readRelation, iids)

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

    fun select(allocator: BufferAllocator, readRelation: RelationReader, path: ByteArray): IntArray {
        val iids =
            bucketer.incrementPath(path)?.let { iids.subSet(bucketer.startIid(path), bucketer.startIid(it)) }
                ?: iids.tailSet(bucketer.startIid(path))

        val res = IntArrayList()
        val iidReader = readRelation["_iid"]

        FixedSizeBinaryVector(allocator, "_iid", false, 16).use { needlesVec ->
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

        return res.toArray()
    }
}