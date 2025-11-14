package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.*
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.error.Incorrect
import xtdb.types.leastUpperBound

class Sum(
    val fromName: FieldName, fromType: VectorType, toName: FieldName, val hasZeroRow: Boolean
) : AggregateSpec.Factory {

    private val lub: ArrowType = leastUpperBound(fromType.unionLegs.map { it.arrowType })
        ?: throw Incorrect("Cannot compute SUM over type $fromType")

    override val field: Field = toName.ofType(maybe(lub))

    override fun build(al: BufferAllocator) = object : AggregateSpec {
        private val outVec = Vector.open(al, field)

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) {
            val inVec = inRel.vectorForOrNull(fromName) ?: return
            val summer = inVec.sumInto(outVec)
            repeat(inRel.rowCount) { idx -> summer.sumRow(idx, groupMapping.getInt(idx)) }
        }

        override fun openFinishedVector(): VectorReader {
            if (hasZeroRow && outVec.valueCount == 0)
                outVec.writeNull()

            return outVec.openSlice(al)
        }

        override fun close() = outVec.close()
    }
}