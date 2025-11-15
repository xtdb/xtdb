package xtdb.arrow

import xtdb.arrow.agg.VectorSummer

sealed class IntegerVector : NumericVector() {

    abstract fun getAsInt(idx: Int): Int
    abstract fun getAsLong(idx: Int): Long

    override fun sumInto(outVec: Vector): VectorSummer =
        when (outVec) {
            is ByteVector -> TODO("sumInto ByteVector not implemented")
            is ShortVector -> TODO("sumInto ShortVector not implemented")

            is IntVector -> VectorSummer { idx, groupIdx ->
                outVec.ensureCapacity(groupIdx + 1)
                if (!isNull(idx))
                    outVec.increment(groupIdx, getAsInt(idx))
            }

            is LongVector -> VectorSummer { idx, groupIdx ->
                outVec.ensureCapacity(groupIdx + 1)
                if (!isNull(idx))
                    outVec.increment(groupIdx, getAsLong(idx))
            }

            else -> super.sumInto(outVec)
        }
}