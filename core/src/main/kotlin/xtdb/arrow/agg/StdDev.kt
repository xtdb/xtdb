package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.*
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.util.closeOnCatch

sealed class StdDev(override val colName: FieldName, private val varianceFactory: AggregateSpec.Factory) : AggregateSpec.Factory {

    override val type: VectorType = maybe(F64)

    override fun build(al: BufferAllocator, args: RelationReader) = object : AggregateSpec {
        private val varianceAgg = varianceFactory.build(al, args)

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) =
            varianceAgg.aggregate(inRel, groupMapping)

        override fun openFinishedVector(): Vector =
            DoubleVector(al, colName, nullable = true).closeOnCatch { outVec ->
                varianceAgg.openFinishedVector().use { it.sqrtInto(outVec) }
            }

        override fun close() = varianceAgg.close()
    }
}

class StdDevPop(fromName: FieldName, toName: FieldName, hasZeroRow: Boolean) :
    StdDev(toName, VariancePop(fromName, "variance", hasZeroRow))

class StdDevSamp(fromName: FieldName, toName: FieldName, hasZeroRow: Boolean) :
    StdDev(toName, VarianceSamp(fromName, "variance", hasZeroRow))
