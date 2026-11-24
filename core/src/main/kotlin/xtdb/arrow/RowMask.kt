package xtdb.arrow

import java.lang.Long.bitCount
import java.lang.Long.expand
import java.lang.Long.numberOfTrailingZeros

/**
 * An immutable on-heap bitmask over [rowCount] rows: row `i` is bit `i % 64` of word `i / 64`.
 */
class RowMask private constructor(private val words: LongArray, val rowCount: Int) {

    val cardinality: Int = words.sumOf { bitCount(it) }

    val wordCount: Int get() = words.size

    fun isSet(row: Int): Boolean = (words[row ushr 6] ushr (row and 63)) and 1L != 0L

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
     * Composes two masks: [refinement] filters the rows this mask kept, and the result is the one mask over this mask's own [rowCount] rows that keeps what both did.
     * It's how a filter over a filter stays one mask deep, as in [MaskedRelation.of].
     *
     * [refinement] is over this mask's set rows, so its [rowCount] MUST be this mask's [cardinality]: its bit `k` keeps or drops this mask's `k`th set row.
     * As selections, `andThen(r).toSelection()` is `toSelection()` indexed by `r.toSelection()`, the way two selections compose.
     */
    fun andThen(refinement: RowMask): RowMask {
        require(refinement.rowCount == cardinality) {
            "refinement is over ${refinement.rowCount} rows, mask sets $cardinality"
        }

        val refWords = refinement.words
        val out = LongArray(words.size)
        // the set rows in the words before `w`: word `w`'s `n` set rows, lowest first, are decided by refinement bits `k until k + n`
        var k = 0
        for (w in words.indices) {
            val word = words[w]
            val n = bitCount(word)
            // nothing to keep, and `out[w]` is already 0.
            // Skipping before the read also matters: after a multiple of 64 set rows, `k ushr 6` is one past the refinement's last word.
            if (n == 0) continue

            // `bits` gets refinement bits `k until k + n` down to its low `n` bits.
            // They start `shift` bits into refinement word `k ushr 6`, and may run off its top:
            // - where `shift + n > 64`, the rest are the low bits of the next refinement word, shifted up to sit above the `64 - shift` taken from this one.
            // - that branch implies `shift > 0` (`n` is at most 64), so the shift is never `shl 64`, which the JVM takes mod 64 as `shl 0`.
            val shift = k and 63
            var bits = refWords[k ushr 6] ushr shift
            if (shift + n > 64) bits = bits or (refWords[(k ushr 6) + 1] shl (64 - shift))

            // `expand(bits, word)` (PDEP) deposits the low bits of `bits`, lowest first, into `word`'s set positions, and clears the rest.
            // So `word`'s `i`th set row survives exactly where bit `i` of `bits` - refinement bit `k + i` - is set.
            // e.g. over 8 bits, `expand(0b0000_0101, 0b0110_1010)`:
            // - `word`'s set positions are 1, 3, 5, 6, and the low four bits of `bits`, lowest first, are 1, 0, 1, 0
            // - so positions 1 and 5 survive: `0b0010_0010`
            // It reads only the low `bitCount(word)` bits, so the refinement bits past `k + n` needn't be masked off.
            out[w] = expand(bits, word)
            k += n
        }
        return RowMask(out, rowCount)
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
        @JvmStatic
        fun all(rowCount: Int): RowMask {
            val words = LongArray((rowCount + 63) ushr 6) { -1L }
            val tailBits = rowCount and 63
            // a partial last word sets only its low `tailBits` bits, the rows below `rowCount`
            if (tailBits != 0) words[words.size - 1] = (1L shl tailBits) - 1
            return RowMask(words, rowCount)
        }

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
