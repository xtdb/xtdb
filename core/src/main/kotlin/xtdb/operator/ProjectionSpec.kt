package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.RelationReader
import xtdb.vector.IVectorReader

interface ProjectionSpec {
    val field: Field

    /**
     * @param args a single-row indirect relation containing the args for this invocation - maybe a view over a bigger arg relation.
     */
    fun project(allocator: BufferAllocator, readRelation: RelationReader, schema: Map<String, Any>, args: RelationReader): IVectorReader
}
