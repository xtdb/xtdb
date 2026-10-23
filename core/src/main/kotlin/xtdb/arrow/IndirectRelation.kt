package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.VectorIndirection.Companion.selection

/**
 * A relation over an [inner] relation and one row selection [sel] into it, held once for the whole relation.
 * `sel == null` is the identity.
 *
 * [select] composes the selection once, however many columns there are; [SelectionSpec]/[ProjectionSpec]
 * implementations that understand this shape read [inner] at `sel[i]` directly (see `refine`).
 *
 * Column views are `IndirectVector(innerVec, sel)` - deliberately NOT flattened via `VectorReader.select`,
 * which would compose an index array per column.
 */
class IndirectRelation(val inner: RelationReader, val sel: IntArray?) : RelationReader {
    override val rowCount: Int get() = sel?.size ?: inner.rowCount

    private val indirection = sel?.let { selection(it) }

    private fun view(v: VectorReader): VectorReader = indirection?.let { IndirectVector(v, it) } ?: v

    override val vectors: Collection<VectorReader> get() = inner.vectors.map(::view)

    override fun vectorForOrNull(name: String): VectorReader? = inner.vectorForOrNull(name)?.let(::view)

    override fun select(idxs: IntArray): RelationReader =
        IndirectRelation(inner, sel?.let { s -> IntArray(idxs.size) { s[idxs[it]] } } ?: idxs)

    override fun select(startIdx: Int, len: Int): RelationReader =
        IndirectRelation(inner, sel?.copyOfRange(startIdx, startIdx + len) ?: IntArray(len) { startIdx + it })

    override fun openSlice(al: BufferAllocator): RelationReader = IndirectRelation(inner.openSlice(al), sel)

    override fun close() = inner.close()

    companion object {
        /**
         * The rows of [rel] to read: its inner relation and selection when it is an [IndirectRelation],
         * else the relation itself under the identity.
         */
        @JvmStatic
        fun innerOf(rel: RelationReader): RelationReader = if (rel is IndirectRelation) rel.inner else rel

        @JvmStatic
        fun selOf(rel: RelationReader): IntArray? = if (rel is IndirectRelation) rel.sel else null
    }
}
