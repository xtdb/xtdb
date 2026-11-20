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
            // takes each set bit lowest first, clearing it once its row is written
            while (word != 0L) {
                sel[i++] = base + numberOfTrailingZeros(word)
                word = word and (word - 1)
            }
        }
        return sel
    }

    /**
     * Builds a [RowMask] over [rowCount] rows from its set rows, added in strictly ascending order.
     * An out-of-order or duplicate add, a row outside `0 until rowCount`, or an add after [build] throws.
     */
    class Builder(private val rowCount: Int) {
        private val words = LongArray((rowCount + 63) ushr 6)

        // the word being filled is buffered in `cur`, and stored at `curWord` once an add moves past it, or on build
        private var curWord = -1
        private var cur = 0L
        private var last = -1

        fun add(row: Int) {
            // the row after the last, in the same word, passes every check below - so a run of consecutive rows skips them
            if (row == last + 1 && row < rowCount && row ushr 6 == curWord) {
                cur = cur or (1L shl (row and 63))
                last = row
                return
            }

            require(row in 0 until rowCount) { "row $row outside 0 until $rowCount" }
            check(row > last) { if (last == BUILT) "row $row added after build" else "row $row added after row $last" }
            last = row

            val w = row ushr 6
            if (w != curWord) {
                if (curWord >= 0) words[curWord] = cur
                curWord = w
                cur = 0L
            }
            cur = cur or (1L shl (row and 63))
        }

        fun build(): RowMask {
            if (curWord >= 0) words[curWord] = cur
            last = BUILT
            return RowMask(words, rowCount)
        }

        private companion object {
            const val BUILT = Int.MAX_VALUE
        }
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

            // the last word's bits from `tailBits` up are past `rowCount`, so shifting the rows below it out has to leave zero
            val tailBits = rowCount and 63
            require(tailBits == 0 || words.last() ushr tailBits == 0L) { "a bit is set at or past row $rowCount" }

            return RowMask(words, rowCount)
        }
    }
}
