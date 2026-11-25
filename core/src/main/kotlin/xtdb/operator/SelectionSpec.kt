package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorIndirection.Mask

interface SelectionSpec : AutoCloseable {
    interface Factory {
        /**
         * @param args a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
         */
        fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader): SelectionSpec
    }

    fun select(readRelation: RelationReader): Mask
}
