package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.vector.RelationReader

interface SelectionSpec {
    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    fun select(allocator: BufferAllocator, readRelation: RelationReader, schema: Map<String, Any>, params: RelationReader): IntArray
}
