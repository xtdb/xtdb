package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.FieldName
import xtdb.arrow.MaskedRelation
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorMask
import xtdb.arrow.VectorType
import xtdb.api.error.Incorrect
import xtdb.types.leastUpperBound

class Sum(
    val fromName: FieldName, override val colName: FieldName, override val type: VectorType, val hasZeroRow: Boolean
) : AggregateSpec.Factory {

    companion object {
        @JvmField
        val MASKED_AGG: Boolean = System.getProperty("xtdb.maskedAgg", "true").toBoolean()

        @JvmStatic
        fun outType(fromType: VectorType) =
            leastUpperBound(listOf(fromType)) ?: throw Incorrect("Cannot compute SUM over type $fromType")
    }

    override fun build(al: BufferAllocator, args: RelationReader) = object : AggregateSpec {
        private val outVec = al.openVector(colName, type)

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping, mask: VectorMask) {
            if (MASKED_AGG && mask === VectorMask.ALL && inRel is MaskedRelation && inRel.isDirect(fromName)) {
                // DART (#6285 avenue B): read the direct vector at each set bit, the group mapping at the logical row
                val summer = inRel.directRel[fromName].sumInto(outVec)
                val m = inRel.mask
                var i = 0
                var p = m.nextSetBit(0)
                while (p >= 0) {
                    val groupIdx = groupMapping.getInt(i++)
                    while (outVec.valueCount <= groupIdx) outVec.writeNull()
                    summer.sumRow(p, groupIdx)
                    p = m.nextSetBit(p + 1)
                }
                return
            }

            val inVec = inRel.vectorForOrNull(fromName) ?: return
            val summer = inVec.sumInto(outVec)

            repeat(inRel.rowCount) { idx ->
                val groupIdx = groupMapping.getInt(idx)
                // a group entirely excluded by the filter never reaches `summer.sumRow`, whose own
                // `ensureCapacity` is otherwise this loop's only padding
                while (outVec.valueCount <= groupIdx) outVec.writeNull()

                if (mask.isSet(idx)) summer.sumRow(idx, groupIdx)
            }
        }

        override fun openFinishedVector(): Vector {
            if (hasZeroRow && outVec.valueCount == 0)
                outVec.writeNull()

            return outVec.openSlice(al)
        }

        override fun close() = outVec.close()
    }
}
