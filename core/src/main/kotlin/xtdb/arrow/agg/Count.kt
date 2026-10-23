package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.FieldName
import xtdb.arrow.LongVector
import xtdb.arrow.MaskedRelation
import xtdb.arrow.MonoVector
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorMask
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.I64

class Count(
    private val fromColName: FieldName,
    override val colName: FieldName,
    private val hasZeroRow: Boolean
) : AggregateSpec.Factory {
    override val type: VectorType = I64

    override fun build(al: BufferAllocator, args: RelationReader): AggregateSpec {
        val outVec = LongVector(al, colName, nullable = false)
        return object : AggregateSpec {

            override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping, mask: VectorMask) {
                if (Sum.MASKED_AGG && mask === VectorMask.ALL && inRel is MaskedRelation && inRel.isDirect(fromColName)) {
                    val col = inRel.directRel[fromColName]
                    val m = inRel.mask
                    var i = 0
                    var p = m.nextSetBit(0)
                    while (p >= 0) {
                        val groupIdx = groupMapping.getInt(i++)
                        if (outVec.valueCount == groupIdx) outVec.writeLong(0L)
                        if (!col.isNull(p)) outVec.setLong(groupIdx, outVec.getLong(groupIdx) + 1)
                        p = m.nextSetBit(p + 1)
                    }
                    return
                }

                val inCol = inRel.vectorForOrNull(fromColName) ?: return

                repeat(inRel.rowCount) { idx ->
                    val groupIdx = groupMapping.getInt(idx)

                    if (outVec.valueCount == groupIdx)
                        outVec.writeLong(0L)

                    if (mask.isSet(idx) && !inCol.isNull(idx)) {
                        val currentCount = if (!outVec.isNull(groupIdx)) outVec.getLong(groupIdx) else 0L
                        outVec.setLong(groupIdx, currentCount + 1)
                    }
                }
            }

            override fun openFinishedVector(): Vector {
                if (hasZeroRow && outVec.valueCount == 0)
                    outVec.writeLong(0L)

                return outVec.openSlice(al)
            }

            override fun close() = outVec.close()
        }
    }
}
