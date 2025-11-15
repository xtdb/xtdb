package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.*
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.util.closeOnCatch

class Average(val fromName: FieldName, toName: FieldName, val hasZeroRow: Boolean) : AggregateSpec.Factory {

    private val f64Type = ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    override val field: Field = toName.ofType(maybe(f64Type))

    override fun build(al: BufferAllocator) = object : AggregateSpec {
        private val sumAgg = Sum(fromName, "sum", f64Type, hasZeroRow).build(al)
        private val countAgg = Count(fromName, "count", hasZeroRow).build(al)

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) {
            sumAgg.aggregate(inRel, groupMapping)
            countAgg.aggregate(inRel, groupMapping)
        }

        override fun openFinishedVector(): VectorReader =
            DoubleVector(al, field.name, nullable = true).closeOnCatch { outVec ->
                sumAgg.openFinishedVector().use { sumVec ->
                    countAgg.openFinishedVector().use { countVec ->
                        (sumVec as DoubleVector).divideInto(countVec, outVec)
                    }
                }
            }

        override fun close() {
            sumAgg.close()
            countAgg.close()
        }
    }
}
