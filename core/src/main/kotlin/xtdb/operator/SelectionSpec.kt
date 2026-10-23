package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader

interface SelectionSpec {
    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    fun select(allocator: BufferAllocator, readRelation: RelationReader, schema: Map<String, Any>, params: RelationReader): IntArray

    /**
     * @param inSel candidate rows of [readRelation], or null for all of them
     * @return the subset of [inSel] (indices into [readRelation]) that pass
     */
    fun selectWithin(
        allocator: BufferAllocator, readRelation: RelationReader, inSel: IntArray?,
        schema: Map<String, Any>, params: RelationReader
    ): IntArray {
        if (inSel == null) return select(allocator, readRelation, schema, params)
        val sel = select(allocator, readRelation.select(inSel), schema, params)
        return IntArray(sel.size) { inSel[sel[it]] }
    }
}
