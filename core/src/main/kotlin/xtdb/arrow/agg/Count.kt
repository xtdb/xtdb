package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.ofType

class Count(
    private val fromColName: FieldName,
    outColName: FieldName,
    private val hasZeroRow: Boolean
) : AggregateSpec.Factory {
    override val field: Field = outColName ofType I64

    override fun build(al: BufferAllocator): AggregateSpec {
        val outVec = Vector.open(al, field)
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

            override fun openFinishedVector(): VectorReader {
                if (hasZeroRow && outVec.valueCount == 0)
                    outVec.writeLong(0L)

                return outVec.openSlice(al)
            }

            override fun close() = outVec.close()
        }
    }
}
