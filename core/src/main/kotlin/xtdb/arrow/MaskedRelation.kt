package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.util.closeOnCatch

/**
 * The rows of [inner] that [mask] sets: logical row `i` is the `i`th set row.
 * Its columns are [MaskedVector]s sharing the one [mask].
 */
class MaskedRelation private constructor(private val inner: RelationReader, internal val mask: RowMask) : RelationReader {

    override val rowCount: Int get() = mask.cardinality

    private val cols: Map<String, VectorReader> =
        inner.vectors.associateByTo(linkedMapOf(), { it.name }) { MaskedVector(it, mask) }

    override val vectors: Collection<VectorReader> get() = cols.values
    override fun vectorForOrNull(name: String) = cols[name]

    override fun select(idxs: IntArray): RelationReader {
        val rows = mask.selection
        return inner.select(IntArray(idxs.size) { rows[idxs[it]] })
    }

    override fun select(startIdx: Int, len: Int): RelationReader =
        inner.select(mask.selection.copyOfRange(startIdx, startIdx + len))

    override fun openSlice(al: BufferAllocator): RelationReader =
        inner.openSlice(al).closeOnCatch { MaskedRelation(it, mask) }

    override fun close() = inner.close()

    companion object {
        /**
         * @param mask is over [rel]'s rows - where [rel] is itself a [MaskedRelation], over the rows its mask sets.
         */
        @JvmStatic
        fun of(rel: RelationReader, mask: RowMask): MaskedRelation = when (rel) {
            is MaskedRelation -> MaskedRelation(rel.inner, rel.mask.andThen(mask))
            else -> MaskedRelation(rel, mask)
        }
    }
}
