package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.IndirectRelation
import xtdb.arrow.RelationReader

interface SelectionSpec {
    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    fun select(allocator: BufferAllocator, readRelation: RelationReader, schema: Map<String, Any>, params: RelationReader): IntArray

    /**
     * Filters [sel] (indices into [inner]; `null` = every row of [inner]) down to the entries whose row passes,
     * returning those *inner* indices - i.e. the new selection, rather than positions within the old one.
     */
    fun refine(
        allocator: BufferAllocator, inner: RelationReader, sel: IntArray?,
        schema: Map<String, Any>, params: RelationReader
    ): IntArray {
        if (sel == null) return select(allocator, inner, schema, params)
        val idxs = select(allocator, IndirectRelation(inner, sel), schema, params)
        return IntArray(idxs.size) { sel[idxs[it]] }
    }

    companion object {
        /**
         * [rel] narrowed to the rows passing [spec], as an [IndirectRelation] whose selection was filtered
         * rather than composed.
         */
        @JvmStatic
        fun selectBy(
            rel: RelationReader, spec: SelectionSpec, allocator: BufferAllocator,
            schema: Map<String, Any>, params: RelationReader
        ): IndirectRelation {
            val inner = IndirectRelation.innerOf(rel)
            return IndirectRelation(inner, spec.refine(allocator, inner, IndirectRelation.selOf(rel), schema, params))
        }
    }
}
