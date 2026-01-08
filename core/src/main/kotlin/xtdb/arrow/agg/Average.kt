package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.*
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.util.closeOnCatch

class Average(val fromName: FieldName, fromType: VectorType, toName: FieldName, val hasZeroRow: Boolean) : AggregateSpec.Factory {

    private val sumOutType = Sum.outType(fromType).let { when(it.arrowType) {
        is ArrowType.Int, is ArrowType.FloatingPoint, is ArrowType.Decimal, is ArrowType.Null -> F64
        is ArrowType.Duration -> it
        else -> throw IllegalArgumentException("Cannot compute AVERAGE over type $fromType")
    } }

    override val field: Field = toName ofType maybe(sumOutType)

    override fun build(al: BufferAllocator) = object : AggregateSpec {
        private val sumAgg = Sum(fromName, "sum", sumOutType, hasZeroRow).build(al)
        private val countAgg = Count(fromName, "count", hasZeroRow).build(al)

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) {
            sumAgg.aggregate(inRel, groupMapping)
            countAgg.aggregate(inRel, groupMapping)
        }

        override fun openFinishedVector(): Vector =
            al.openVector(field).closeOnCatch { outVec ->
                sumAgg.openFinishedVector().use { sumVec ->
                    countAgg.openFinishedVector().use { countVec ->
                        sumVec.divideInto(countVec, outVec)
                    }
                }
            }

        override fun close() {
            countAgg.close()
            sumAgg.close()
        }
    }
}
