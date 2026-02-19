package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.FieldName
import xtdb.arrow.LongVector
import xtdb.arrow.MonoVector
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.I64

class RowCount(override val colName: FieldName, val hasZeroRow: Boolean) : AggregateSpec.Factory {
    override val type: VectorType = I64

    override fun build(al: BufferAllocator, args: RelationReader): AggregateSpec {
        val outVec = LongVector(al, colName, false)

        return object : AggregateSpec {

            override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) {
                repeat(inRel.rowCount) { idx ->
                    val groupIdx = groupMapping.getInt(idx)

                    if (outVec.valueCount == groupIdx)
                        outVec.writeLong(0L)

                    outVec.setLong(groupIdx, outVec.getLong(groupIdx) + 1)
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