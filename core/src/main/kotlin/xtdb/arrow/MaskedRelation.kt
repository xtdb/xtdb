package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.VectorIndirection.Mask
import xtdb.util.closeOnCatch

/**
 * The rows of [inner] that [mask] sets: logical row `i` is the `i`th set row.
 */
class MaskedRelation internal constructor(private val inner: RelationReader, internal val mask: Mask) : RelationReader {

    override val rowCount: Int get() = mask.cardinality

    override val cols: Map<String, VectorReader> =
        inner.cols.mapValuesTo(linkedMapOf()) { it.value.select(mask) }

    override fun vectorForOrNull(name: String) = cols[name]

    override fun select(idxs: IntArray): RelationReader {
        val rows = mask.idxs
        return inner.select(IntArray(idxs.size) { rows[idxs[it]] })
    }

    override fun select(mask: Mask): RelationReader = MaskedRelation(inner, this.mask.andThen(mask))

    override fun openSlice(al: BufferAllocator): RelationReader =
        inner.openSlice(al).closeOnCatch { MaskedRelation(it, mask) }

    override fun close() = inner.close()
}
