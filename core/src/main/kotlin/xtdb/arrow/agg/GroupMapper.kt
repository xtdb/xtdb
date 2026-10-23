package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.MaskedRelation
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.operator.distinct.DistinctRelationMap
import xtdb.operator.distinct.DistinctRelationMap.Companion.insertedIdx

typealias GroupMapping = VectorReader

interface GroupMapper : AutoCloseable {
    fun groupMapping(inRelation: RelationReader): GroupMapping
    fun finish(): RelationReader

    class Null(al: BufferAllocator) : GroupMapper {
        private val groupMapping = al.openVector("group-mapping", I32)

        override fun groupMapping(inRelation: RelationReader): GroupMapping {
            groupMapping.clear()
            repeat(inRelation.rowCount) {
                groupMapping.writeInt(0)
            }

            return groupMapping
        }

        override fun finish(): RelationReader = RelationReader.from(emptyList(), 1)

        override fun close() = groupMapping.close()
    }

    class Mapper(al: BufferAllocator, private val relMap: DistinctRelationMap) : GroupMapper {
        private val groupMapping = al.openVector("group-mapping", I32)

        override fun groupMapping(inRelation: RelationReader): GroupMapping {
            groupMapping.clear()

            if (Sum.MASKED_AGG && inRelation is MaskedRelation && relMap.keyColumnNames.all { inRelation.isDirect(it) }) {
                // DART (#6285 avenue B): hash the direct key vectors at each set bit
                val builder = relMap.buildFromRelation(inRelation.directRel)
                val m = inRelation.mask
                var p = m.nextSetBit(0)
                while (p >= 0) {
                    groupMapping.writeInt(insertedIdx(builder.addIfNotPresent(p)))
                    p = m.nextSetBit(p + 1)
                }
                return groupMapping
            }

            val builder = relMap.buildFromRelation(inRelation)

            repeat(inRelation.rowCount) { idx ->
                groupMapping.writeInt(insertedIdx(builder.addIfNotPresent(idx)))
            }

            return groupMapping
        }

        override fun finish(): RelationReader = relMap.getBuiltRelation()

        override fun close() {
            groupMapping.close()
            relMap.close()
        }
    }
}
