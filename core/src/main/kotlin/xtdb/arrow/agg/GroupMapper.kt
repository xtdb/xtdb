package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
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
        private val groupMapping = Vector.open(al, "group-mapping" ofType I32)

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
        private val groupMapping = Vector.open(al, "group-mapping" ofType I32)

        override fun groupMapping(inRelation: RelationReader): GroupMapping {
            groupMapping.clear()
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
