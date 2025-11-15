package xtdb.arrow

import xtdb.arrow.agg.VectorSummer

sealed class NumericVector : FixedWidthVector() {

    abstract fun getAsFloat(idx: Int): Float
    abstract fun getAsDouble(idx: Int): Double

    override fun sumInto(outVec: Vector): VectorSummer =
        when (outVec) {
            is FloatVector -> VectorSummer { idx, groupIdx ->
                outVec.ensureCapacity(groupIdx + 1)
                if (!isNull(idx))
                    outVec.increment(groupIdx, getAsFloat(idx))
            }

            is DoubleVector -> VectorSummer { idx, groupIdx ->
                outVec.ensureCapacity(groupIdx + 1)
                if (!isNull(idx))
                    outVec.increment(groupIdx, getAsDouble(idx))
            }

            else -> unsupported("sumInto ${outVec.arrowType}")
        }
}