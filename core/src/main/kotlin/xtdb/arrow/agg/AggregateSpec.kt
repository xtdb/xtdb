package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.VectorType

interface AggregateSpec : AutoCloseable {
    interface Factory {
        val colName: String
        val type: VectorType
        fun build(al: BufferAllocator): AggregateSpec
    }

    fun aggregate(inRel: RelationReader, groupMapping: GroupMapping)
    fun openFinishedVector(): Vector
}