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

    /**
     * The rows of [mask] that this selects: rows [mask] doesn't set are never selected, and needn't be evaluated.
     *
     * @param mask over [readRelation]'s rows.
     */
    fun select(readRelation: RelationReader, mask: Mask): Mask

    fun select(readRelation: RelationReader): Mask = select(readRelation, Mask.all(readRelation.rowCount))
}
