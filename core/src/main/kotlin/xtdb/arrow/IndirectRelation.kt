package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorIndirection.Companion.slice
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.safeMap

/**
 * A relation over direct (unselected) vectors, with the row selection held once per [Group] rather than per column.
 * The per-column [IndirectVector] views are built on demand.
 *
 * A group's `sel` of `null` means the identity: its vectors are already indexed by relation row.
 * More than one group arises where columns come from differently-indexed sources (the scan's content columns
 * index into the leaf, its temporal columns are dense);
 * [select] composes each group's selection once, however many columns it holds.
 *
 * Closing this relation closes the direct vectors, as closing each of the views would; a relation derived by [select]
 * shares them, so only one of the two is closed.
 */
class IndirectRelation private constructor(
    private val groups: List<Group>, override val rowCount: Int
) : RelationReader {

    class Group(val direct: List<VectorReader>, val sel: VectorIndirection?) {
        private val byName = direct.associateBy { it.name }

        fun view(name: String): VectorReader? =
            byName[name]?.let(::view)

        fun views(): List<VectorReader> = direct.map(::view)

        // `select` rather than `IndirectVector(v, sel)` so that a direct vector that is itself indirect is flattened
        private fun view(v: VectorReader): VectorReader = when (sel) {
            null -> v
            is VectorIndirection.Companion.Slice -> v.select(sel.startIdx, sel.len)
            is VectorIndirection.Companion.Selection -> v.select(sel.idxs)
            else -> IndirectVector(v, sel)
        }

        fun select(idxs: IntArray) = Group(direct, sel?.let { selection(it.select(idxs)) } ?: selection(idxs))
        fun select(startIdx: Int, len: Int) = Group(direct, sel?.let { selection(it.select(startIdx, len)) } ?: slice(startIdx, len))
    }

    override val vectors: Collection<VectorReader> get() = groups.flatMap { it.views() }

    override fun vectorForOrNull(name: String): VectorReader? {
        for (g in groups) g.view(name)?.let { return it }
        return null
    }

    override fun select(idxs: IntArray): RelationReader = IndirectRelation(groups.map { it.select(idxs) }, idxs.size)

    override fun select(startIdx: Int, len: Int): RelationReader =
        IndirectRelation(groups.map { it.select(startIdx, len) }, len)

    override fun openSlice(al: BufferAllocator): RelationReader =
        groups.flatMap { it.direct }.safeMap { it.openSlice(al) }.closeAllOnCatch { sliced ->
            val it = sliced.iterator()
            IndirectRelation(groups.map { g -> Group(g.direct.map { _ -> it.next() }, g.sel) }, rowCount)
        }

    override fun close() = groups.forEach { it.direct.closeAll() }

    companion object {
        @JvmStatic
        fun of(groups: List<Group>, rowCount: Int) = IndirectRelation(groups, rowCount)
    }
}
