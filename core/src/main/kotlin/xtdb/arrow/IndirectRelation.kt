package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorIndirection.Mask
import xtdb.util.closeOnCatch

/**
 * The rows of [inner] that [sel] picks: logical row `i` is [inner]'s row `sel[i]`.
 */
class IndirectRelation internal constructor(
    private val inner: RelationReader, internal val sel: VectorIndirection
) : RelationReader {

    override val rowCount: Int get() = sel.valueCount()

    override val cols: Map<String, VectorReader> =
        inner.cols.mapValuesTo(linkedMapOf()) { sel.selectFrom(it.value) }

    override fun vectorForOrNull(name: String) = cols[name]

    override fun select(idxs: IntArray): RelationReader = IndirectRelation(inner, selection(sel.select(idxs)))

    override fun select(mask: Mask): RelationReader = IndirectRelation(inner, sel.select(mask))

    override fun openSlice(al: BufferAllocator): RelationReader =
        inner.openSlice(al).closeOnCatch { IndirectRelation(it, sel) }

    override fun close() = inner.close()
}
