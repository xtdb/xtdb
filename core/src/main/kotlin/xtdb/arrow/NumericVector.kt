package xtdb.arrow

import com.carrotsearch.hppc.DoubleArrayList
import xtdb.arrow.VectorIndirection.Mask
import xtdb.arrow.agg.VectorSummer
import xtdb.hash.doubleHashes
import xtdb.util.Hasher

sealed class NumericVector : FixedWidthVector() {

    abstract fun getAsFloat(idx: Int): Float
    abstract override fun getAsDouble(idx: Int): Double

    /**
     * Appends each row's value (of [sel], else of this vector) as `hashCode0` hashes it; a null row's value is ignored.
     */
    internal abstract fun doublesTo(out: DoubleArrayList, sel: IntArray?)

    override fun compare(op: CmpOp, rhs: Long, sel: IntArray?, mask: Mask): Mask? = compare(op, rhs.toDouble(), sel, mask)

    abstract override fun compare(op: CmpOp, rhs: Double, sel: IntArray?, mask: Mask): Mask?

    final override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) =
        doubleHashes(this, out, sel, hasher) { doublesTo(it, sel) }

    override fun sumInto(outVec: Vector): VectorSummer =
        when (outVec) {
            is FloatVector -> VectorSummer { idx, groupIdx ->
                if (!isNull(idx))
                    outVec.increment(groupIdx, getAsFloat(idx))
            }

            is DoubleVector -> VectorSummer { idx, groupIdx ->
                if (!isNull(idx))
                    outVec.increment(groupIdx, getAsDouble(idx))
            }

            else -> unsupported("sumInto ${outVec.arrowType}")
        }
}