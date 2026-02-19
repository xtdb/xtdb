package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.Bytes
import xtdb.arrow.*
import xtdb.arrow.Vector.Companion.openVector

class VarWidthMinMax(
    private val fromName: FieldName,
    override val colName: FieldName,
    override val type: VectorType,
    private val hasZeroRow: Boolean,
    private val isMin: Boolean,
) : AggregateSpec.Factory {

    override fun build(al: BufferAllocator, args: RelationReader) = object : AggregateSpec {
        private val winners = ArrayList<ByteArray?>()

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping) {
            val inVec = inRel.vectorForOrNull(fromName) ?: return

            repeat(inRel.rowCount) { idx ->
                val groupIdx = groupMapping.getInt(idx)

                while (winners.size <= groupIdx)
                    winners.add(null)

                if (!inVec.isNull(idx)) {
                    val bb = inVec.getBytes(idx)
                    val newBytes = ByteArray(bb.remaining()).also { bb.duplicate().get(it) }
                    val current = winners[groupIdx]

                    if (current == null || Bytes.COMPARATOR.compare(newBytes, current).let { if (isMin) it < 0 else it > 0 }) {
                        winners[groupIdx] = newBytes
                    }
                }
            }
        }

        override fun openFinishedVector(): Vector {
            val outVec = al.openVector(colName, type)

            for (winner in winners) {
                if (winner == null) outVec.writeNull()
                else outVec.writeBytes(winner)
            }

            if (hasZeroRow && winners.isEmpty())
                outVec.writeNull()

            return outVec
        }

        override fun close() {}
    }
}
