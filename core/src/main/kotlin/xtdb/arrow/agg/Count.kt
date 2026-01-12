package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.I64

class Count(
    private val fromColName: FieldName,
    override val colName: FieldName,
    private val hasZeroRow: Boolean
) : AggregateSpec.Factory {
    override val type: VectorType = I64

    override fun build(al: BufferAllocator): AggregateSpec {
        val outVec = al.openVector(field)
        return object : AggregateSpec {

            override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) {
                val inCol = inRel.vectorForOrNull(fromColName) ?: return

                repeat(inRel.rowCount) { idx ->
                    val groupIdx = groupMapping.getInt(idx)

                    if (outVec.valueCount == groupIdx)
                        outVec.writeLong(0L)

                    if (!inCol.isNull(idx)) {
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
