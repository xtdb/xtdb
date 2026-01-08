package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.*
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.error.Incorrect
import xtdb.types.leastUpperBound

class Sum(
    val fromName: FieldName, toName: FieldName, toType: VectorType, val hasZeroRow: Boolean
) : AggregateSpec.Factory {

    override val field: Field = toName ofType toType

    companion object {
        @JvmStatic
        fun outType(fromType: VectorType) =
            maybe(leastUpperBound(listOf(fromType))
                ?: throw Incorrect("Cannot compute SUM over type $fromType"))
    }

    override fun build(al: BufferAllocator) = object : AggregateSpec {
        private val outVec = al.openVector(field)

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
