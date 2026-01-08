package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.ofType

class RowCount(outColName: FieldName, val hasZeroRow: Boolean) : AggregateSpec.Factory {
    override val field: Field = outColName ofType I64

    override fun build(al: BufferAllocator): AggregateSpec {
        val outVec = al.openVector(field)
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