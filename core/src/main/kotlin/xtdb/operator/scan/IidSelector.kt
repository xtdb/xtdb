package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.ArrowUtil.openArrowBufView
import xtdb.operator.SelectionSpec
import xtdb.vector.RelationReader
import java.nio.ByteBuffer
import java.util.stream.IntStream

class IidSelector(private val iid: ByteBuffer) : SelectionSpec {

    override fun select(
        allocator: BufferAllocator,
        readRelation: RelationReader,
        schema: Map<String, Any>,
        params: RelationReader
    ): IntArray =
        iid.openArrowBufView(allocator).use { iidBuf ->
            val iidPtr = ArrowBufPointer(iidBuf, 0, iid.capacity().toLong())
            val ptr = ArrowBufPointer()
            val iidReader = readRelation.readerForName("_iid")
            val valueCount = iidReader.valueCount
            if (valueCount == 0) return@use IntArray(0)

            var left = 0
            var right = valueCount - 1

            // lower bound
            while (left < right) {
                val mid = (left + right) / 2
                if (iidPtr <= iidReader.getPointer(mid, ptr)) right = mid else left = mid + 1
            }

            // upper bound
            if (iidPtr == iidReader.getPointer(left, ptr)) {
                while (right < valueCount && iidPtr == iidReader.getPointer(right, ptr))
                    right++
            }

            IntStream.range(left, right).toArray()
        }
}