package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Scalar
import xtdb.error.Incorrect
import xtdb.types.leastUpperBound

class Sum(
    val fromName: FieldName, override val colName: FieldName, override val type: VectorType, val hasZeroRow: Boolean
) : AggregateSpec.Factory {

    companion object {
        @JvmStatic
        fun outType(fromType: VectorType) =
            leastUpperBound(listOf(fromType)) ?: throw Incorrect("Cannot compute SUM over type $fromType")
    }

    override fun build(al: BufferAllocator) = object : AggregateSpec {
        private val outVec = al.openVector(colName, type)

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) {
            val inVec = inRel.vectorForOrNull(fromName) ?: return
            val summer = inVec.sumInto(outVec)
            repeat(inRel.rowCount) { idx -> summer.sumRow(idx, groupMapping.getInt(idx)) }
        }

        override fun openFinishedVector(): Vector {
            if (hasZeroRow && outVec.valueCount == 0)
                outVec.writeNull()

            return outVec.openSlice(al)
        }

        override fun close() = outVec.close()
    }
}
