package xtdb.operator.scan

import xtdb.arrow.LongLongVectorReader
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorIndirection.Mask
import xtdb.operator.SelectionSpec
import xtdb.util.requireIid
import java.nio.ByteBuffer

class SingleIidSelector(iid: ByteArray) : SelectionSpec {

    private val iidHigh: Long
    private val iidLow: Long

    init {
        requireIid(iid)

        val iidBuf = ByteBuffer.wrap(iid)
        iidHigh = iidBuf.getLong(0)
        iidLow = iidBuf.getLong(8)
    }

    override fun select(readRelation: RelationReader, mask: Mask): Mask {
        val iidReader = readRelation["_iid"] as LongLongVectorReader
        val valueCount = iidReader.valueCount
        val res = Mask.Builder(valueCount)
        if (valueCount == 0) return res.build()

        var left = 0
        var right = valueCount - 1

        // lower bound
        while (left < right) {
            val mid = (left + right) / 2
            if (iidReader.compareAt(mid, iidHigh, iidLow) >= 0) right = mid else left = mid + 1
        }

        // upper bound
        if (iidReader.compareAt(left, iidHigh, iidLow) == 0) {
            while (right < valueCount && iidReader.compareAt(right, iidHigh, iidLow) == 0)
                right++
        }

        for (row in left until right) res.add(row)
        return res.build() and mask
    }

    override fun close() = Unit
}
