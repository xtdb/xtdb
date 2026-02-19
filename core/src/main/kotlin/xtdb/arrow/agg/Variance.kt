package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.*
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Maybe
import xtdb.util.closeOnCatch
import kotlin.math.max

sealed class Variance(
    val fromName: FieldName,
    override val colName: FieldName,
    val hasZeroRow: Boolean,
    private val isSample: Boolean
) : AggregateSpec.Factory {

    override val type: VectorType = Maybe(F64)

    override fun build(al: BufferAllocator, args: RelationReader) = object : AggregateSpec {
        private val sumxAgg = Sum(fromName, "sumx", F64, hasZeroRow).build(al, args)
        private val sumx2Agg = Sum("x2", "sumx2", F64, hasZeroRow).build(al, args)
        private val countAgg = Count(fromName, "count", hasZeroRow).build(al, args)

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) {
            val inVec = inRel.vectorForOrNull(fromName) ?: return

            DoubleVector(al, "x2", nullable = true)
                .closeOnCatch { inVec.squareInto(it) }
                .use { x2Vec ->
                    sumxAgg.aggregate(inRel, groupMapping)
                    sumx2Agg.aggregate(RelationReader.from(listOf(x2Vec), inRel.rowCount), groupMapping)
                    countAgg.aggregate(inRel, groupMapping)
                }
        }

        override fun openFinishedVector(): Vector {
            val outVec = al.openVector(colName, type) as DoubleVector

            sumxAgg.openFinishedVector().use { sumxVec ->
                sumx2Agg.openFinishedVector().use { sumx2Vec ->
                    countAgg.openFinishedVector().use { countVec ->
                        repeat(max(sumxVec.valueCount, max(sumx2Vec.valueCount, countVec.valueCount))) { idx ->
                            if (sumxVec.isNull(idx) || sumx2Vec.isNull(idx) || countVec.isNull(idx)) {
                                outVec.writeNull()
                            } else {
                                val sumx = sumxVec.getDouble(idx)
                                val sumx2 = sumx2Vec.getDouble(idx)
                                val count = countVec.getLong(idx)

                                if (count < (if (isSample) 2 else 1)) {
                                    outVec.writeNull()
                                } else {
                                    val divisor = if (isSample) (count - 1).toDouble() else count.toDouble()
                                    outVec.writeDouble((sumx2 - (sumx * sumx) / count) / divisor)
                                }
                            }
                        }
                    }
                }
            }

            if (hasZeroRow && outVec.valueCount == 0)
                outVec.writeNull()

            return outVec
        }

        override fun close() {
            sumxAgg.close()
            sumx2Agg.close()
            countAgg.close()
        }
    }
}

class VariancePop(fromName: FieldName, toName: FieldName, hasZeroRow: Boolean) :
    Variance(fromName, toName, hasZeroRow, false)

class VarianceSamp(fromName: FieldName, toName: FieldName, hasZeroRow: Boolean) :
    Variance(fromName, toName, hasZeroRow, true)
