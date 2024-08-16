package xtdb.operator

import clojure.lang.Symbol
import org.apache.arrow.memory.BufferAllocator
import xtdb.vector.RelationReader
import xtdb.vector.IVectorReader

interface ProjectionSpec {
    val columnName: Symbol
    val columnType: Any

    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    fun project(allocator: BufferAllocator, readRelation: RelationReader, params: RelationReader): IVectorReader
}
