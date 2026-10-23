package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.operator.distinct.DistinctRelationMap
import xtdb.operator.distinct.DistinctRelationMap.Companion.insertedIdx

typealias GroupMapping = VectorReader

@JvmField
val SEL_AWARE = !java.lang.Boolean.getBoolean("xtdb.noSelAware")

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

            val keyCols = relMap.keyColumnNames.map { inRelation[it] }
            val sel = keyCols.firstOrNull()?.selIdxs

            if (sel != null && SEL_AWARE && !relMap.storeFullBuildRel && keyCols.all { it.selIdxs === sel }) {
                val directCols = keyCols.map { it.direct }
                val builder = relMap.buildFromRelation(RelationReader.from(directCols, directCols.first().valueCount))

                repeat(inRelation.rowCount) { idx ->
                    groupMapping.writeInt(insertedIdx(builder.addIfNotPresent(sel[idx])))
                }
            } else {
                val builder = relMap.buildFromRelation(inRelation)

                repeat(inRelation.rowCount) { idx ->
                    groupMapping.writeInt(insertedIdx(builder.addIfNotPresent(idx)))
                }
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
