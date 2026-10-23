package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.safeMap
import java.util.BitSet

/**
 * DART (#6285 avenue B): a relation over direct (physical) vectors plus one bitmask over their rows,
 * alongside 'dense' vectors already in logical-row space (one value per set bit, in bit order).
 *
 * Mask-aware consumers iterate the set bits ([mask].nextSetBit) and read [direct] at the physical index,
 * [dense] at the logical (running) index.
 * Everyone else gets the RelationReader view: the mask resolved once to an `int[]` selection,
 * shared by every direct column.
 */
class MaskedRelation(
    val direct: List<VectorReader>,
    val physicalCount: Int,
    val mask: BitSet,
    val dense: List<VectorReader>,
) : RelationReader {

    override val rowCount: Int = mask.cardinality()

    val directRel: RelationReader by lazy { RelationReader.from(direct, physicalCount) }

    fun isDirect(name: String) = direct.any { it.name == name }

    val selection: IntArray by lazy {
        val sel = IntArray(rowCount)
        var i = 0
        var idx = mask.nextSetBit(0)
        while (idx >= 0) {
            sel[i++] = idx
            idx = mask.nextSetBit(idx + 1)
        }
        sel
    }

    private val views = HashMap<String, VectorReader>()

    private fun view(v: VectorReader) = views.getOrPut(v.name) { v.select(selection) }

    override val vectors: Collection<VectorReader> get() = direct.map(::view) + dense

    override fun vectorForOrNull(name: String): VectorReader? =
        dense.firstOrNull { it.name == name } ?: direct.firstOrNull { it.name == name }?.let(::view)

    override fun select(idxs: IntArray): RelationReader {
        val sel = selection
        val composed = IntArray(idxs.size) { sel[idxs[it]] }
        return RelationReader.from(direct.map { it.select(composed) } + dense.map { it.select(idxs) }, idxs.size)
    }

    fun withMask(newMask: BitSet) = MaskedRelation(direct, physicalCount, newMask, dense)

    fun withVectors(direct: List<VectorReader>, dense: List<VectorReader>) =
        MaskedRelation(direct, physicalCount, mask, dense)

    override fun openSlice(al: BufferAllocator): RelationReader =
        direct.safeMap { it.openSlice(al) }.closeAllOnCatch { d ->
            dense.safeMap { it.openSlice(al) }.closeAllOnCatch { ds ->
                MaskedRelation(d, physicalCount, mask, ds)
            }
        }

    override fun close() {
        direct.closeAll()
        dense.closeAll()
    }
}
