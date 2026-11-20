package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.arrow.RowMask

interface RowMaskSpec {
    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    fun rowMask(allocator: BufferAllocator, readRelation: RelationReader, schema: Map<String, Any>, params: RelationReader): RowMask
}
