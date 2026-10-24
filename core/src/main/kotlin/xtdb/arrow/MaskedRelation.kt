package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.safeMap

/**
 * DART (#6285 avenue B): a relation over direct (physical) vectors plus one bitmask over their rows,
 * alongside 'dense' vectors already in logical-row space (one value per set bit, in bit order).
 *
 * Mask-aware consumers iterate the set bits ([forEachSetBit] over [words]) and read [direct] at the physical index,
 * [dense] at the logical (running) index.
 * Everyone else gets the RelationReader view: the mask resolved once to an `int[]` selection,
 * shared by every direct column.
 *
 * [words] is treated as immutable once the relation is built - refining a mask copies it.
 */
class MaskedRelation(
    val direct: List<VectorReader>,
    val physicalCount: Int,
    val words: LongArray,
    val dense: List<VectorReader>,
) : RelationReader {

    override val rowCount: Int = words.sumOf { java.lang.Long.bitCount(it) }

    private val directByName: Map<String, VectorReader> = direct.associateBy { it.name }
    private val denseByName: Map<String, VectorReader> = dense.associateBy { it.name }

    val directRel: RelationReader by lazy { RelationReader.from(direct, physicalCount) }

    fun isDirect(name: String) = directByName.containsKey(name)
    fun directVector(name: String): VectorReader = directByName[name] ?: error("not direct: $name")

    val selection: IntArray by lazy {
        val sel = IntArray(rowCount)
        var i = 0
        forEachSetBit(words) { sel[i++] = it }
        sel
    }

    private val views = HashMap<String, VectorReader>()

    private fun view(v: VectorReader) = views.getOrPut(v.name) { v.select(selection) }

    override val vectors: Collection<VectorReader> get() = direct.map(::view) + dense

    override fun vectorForOrNull(name: String): VectorReader? =
        denseByName[name] ?: directByName[name]?.let(::view)

    override fun select(idxs: IntArray): RelationReader {
        val sel = selection
        val composed = IntArray(idxs.size) { sel[idxs[it]] }
        return RelationReader.from(direct.map { it.select(composed) } + dense.map { it.select(idxs) }, idxs.size)
    }

    fun withWords(newWords: LongArray) = MaskedRelation(direct, physicalCount, newWords, dense)

    fun withVectors(direct: List<VectorReader>, dense: List<VectorReader>) =
        MaskedRelation(direct, physicalCount, words, dense)

    override fun openSlice(al: BufferAllocator): RelationReader =
        direct.safeMap { it.openSlice(al) }.closeAllOnCatch { d ->
            dense.safeMap { it.openSlice(al) }.closeAllOnCatch { ds ->
                MaskedRelation(d, physicalCount, words, ds)
            }
        }

    override fun close() {
        direct.closeAll()
        dense.closeAll()
    }

    companion object {
        @JvmStatic
        fun wordCount(physicalCount: Int) = (physicalCount + 63) ushr 6

        inline fun forEachSetBit(words: LongArray, f: (Int) -> Unit) {
            for (wi in words.indices) {
                var w = words[wi]
                val base = wi shl 6
                while (w != 0L) {
                    f(base + java.lang.Long.numberOfTrailingZeros(w))
                    w = w and (w - 1)
                }
            }
        }
    }
}
