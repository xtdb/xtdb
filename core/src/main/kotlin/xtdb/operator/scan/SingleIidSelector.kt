package xtdb.operator.scan

import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.ByteFunctionHelpers
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorIndirection.Mask
import xtdb.operator.SelectionSpec

class SingleIidSelector(private val iid: ByteArray) : SelectionSpec {

    private operator fun ArrowBufPointer.compareTo(bytes: ByteArray): Int =
        ByteFunctionHelpers.compare(
            this.buf, this.offset.toInt(), this.offset.toInt() + bytes.size,
            bytes, 0, bytes.size
        )

    private operator fun ByteArray.compareTo(pointer: ArrowBufPointer) = - (pointer.compareTo(this))

    override fun select(readRelation: RelationReader, mask: Mask): Mask {
        val ptr = ArrowBufPointer()
        val iidReader = readRelation["_iid"]
        val valueCount = iidReader.valueCount
        val res = Mask.Builder(valueCount)
        if (valueCount == 0) return res.build()

        var left = 0
        var right = valueCount - 1

        // lower bound
        while (left < right) {
            val mid = (left + right) / 2
            if (iid <= iidReader.getPointer(mid, ptr)) right = mid else left = mid + 1
        }

        // upper bound
        if (iid.compareTo(iidReader.getPointer(left, ptr)) == 0) {
            while (right < valueCount && iid.compareTo(iidReader.getPointer(right, ptr)) == 0)
                right++
        }

        for (row in left until right) res.add(row)
        return res.build() and mask
    }

    override fun close() = Unit
}