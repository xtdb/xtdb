package xtdb.operator

import clojure.lang.IPersistentMap
import org.apache.arrow.memory.BufferAllocator
import xtdb.vector.RelationReader

interface IRelationSelector {
    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    fun select(allocator: BufferAllocator, readRelation: RelationReader, schema: IPersistentMap, params: RelationReader): IntArray
}
