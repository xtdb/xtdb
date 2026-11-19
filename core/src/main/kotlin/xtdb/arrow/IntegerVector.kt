package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.agg.VectorSummer

sealed class IntegerVector : NumericVector() {

    abstract fun getAsInt(idx: Int): Int
    abstract override fun getAsLong(idx: Int): Long

    abstract override fun openSlice(al: BufferAllocator): IntegerVector

    override fun sumInto(outVec: Vector): VectorSummer =
        when (outVec) {
            is ByteVector -> TODO("sumInto ByteVector not implemented")
            is ShortVector -> TODO("sumInto ShortVector not implemented")

            is IntVector -> VectorSummer { idx, groupIdx ->
                if (!isNull(idx))
                    outVec.increment(groupIdx, getAsInt(idx))
            }

            is LongVector -> VectorSummer { idx, groupIdx ->
                if (!isNull(idx))
                    outVec.increment(groupIdx, getAsLong(idx))
            }

            else -> super.sumInto(outVec)
        }
}