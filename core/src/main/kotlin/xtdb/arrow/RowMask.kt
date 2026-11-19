package xtdb.arrow

import java.lang.Long.bitCount
import java.lang.Long.numberOfTrailingZeros

/**
 * An immutable on-heap bitmask over [rowCount] rows: row `i` is bit `i % 64` of word `i / 64`.
 */
class RowMask private constructor(private val words: LongArray, val rowCount: Int) {

    val cardinality: Int = words.sumOf { bitCount(it) }

    val wordCount: Int get() = words.size

    /** Rows `64 * w` to `64 * w + 63`, row `64 * w` in the low bit. */
    fun word(w: Int): Long = words[w]

    /**
     * [toSelection], built once and shared by every reader of this mask.
     * Callers MUST NOT write to it.
     */
    internal val selection: IntArray by lazy(LazyThreadSafetyMode.PUBLICATION) { toSelection() }

    /**
     * The set rows, ascending.
     */
    fun toSelection(): IntArray {
        val sel = IntArray(cardinality)
        var i = 0
        for (w in words.indices) {
            var word = words[w]
            val base = w shl 6
            while (word != 0L) {
                sel[i++] = base + numberOfTrailingZeros(word)
                word = word and (word - 1)
            }
        }
        return sel
    }

    /**
     * The rows of this mask that [refinement] keeps, over this mask's [rowCount] rows.
     *
     * [refinement] is over this mask's set rows: its bit `k` decides the `k`th of them.
     */
    fun refine(refinement: RowMask): RowMask {
        require(refinement.rowCount == cardinality) {
            "refinement is over ${refinement.rowCount} rows, mask sets $cardinality"
        }

        val out = LongArray(words.size)
        var k = 0
        for (w in words.indices) {
            var word = words[w]
            var kept = 0L
            while (word != 0L) {
                val lowest = word and -word
                if ((refinement.words[k ushr 6] ushr (k and 63)) and 1L != 0L) kept = kept or lowest
                k++
                word = word xor lowest
            }
            out[w] = kept
        }
        return RowMask(out, rowCount)
    }

    companion object {
        /**
         * Wraps [words] without copying: the caller MUST NOT write to them afterwards.
         *
         * @param words row `i` is bit `i % 64` of word `i / 64`, one word per 64 rows (or part thereof).
         * @throws IllegalArgumentException if there isn't exactly one word per 64 rows, or a bit at or past [rowCount] is set.
         */
        @JvmStatic
        fun ofWords(words: LongArray, rowCount: Int): RowMask {
            require(rowCount >= 0) { "negative rowCount $rowCount" }
            require(words.size == (rowCount + 63) ushr 6) { "${words.size} words over $rowCount rows" }

            val tailBits = rowCount and 63
            require(tailBits == 0 || words.last() ushr tailBits == 0L) { "a bit is set at or past row $rowCount" }

            return RowMask(words, rowCount)
        }
    }
}
