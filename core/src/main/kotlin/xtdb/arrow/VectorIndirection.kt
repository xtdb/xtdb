package xtdb.arrow

import java.lang.Long.bitCount
import java.lang.Long.compress
import java.lang.Long.expand
import java.lang.Long.numberOfTrailingZeros

interface VectorIndirection {
    fun valueCount(): Int

    fun getIndex(idx: Int): Int

    /**
     * [getIndex] of each logical row, in order.
     * Callers MUST NOT write to it.
     */
    val idxs: IntArray

    operator fun get(idx: Int): Int = getIndex(idx)

    fun select(idxs: IntArray) = IntArray(idxs.size) { getIndex(idxs[it]) }

    /**
     * The rows of this indirection that [mask] sets, as one indirection over the same inner rows.
     * [mask] is over this indirection's logical rows, so its [Mask.rowCount] MUST be [valueCount].
     */
    fun select(mask: Mask): VectorIndirection

    /**
     * [vec]'s rows through this indirection: logical row `i` is [vec]'s row `this[i]`.
     * Goes through [vec]'s own `select`, so an indirection [vec] already has composes with this one rather than nesting.
     */
    fun selectFrom(vec: VectorReader): VectorReader

    abstract class Iterator : IntIterator() {
        /**
         * Positions the iterator so that the next [nextInt] returns the index for logical row [idx].
         */
        abstract fun seek(idx: Int)
    }

    operator fun iterator(): Iterator

    companion object {
        @JvmStatic
        fun selection(idxs: IntArray): VectorIndirection = Selection(idxs)

        internal data class Selection(override val idxs: IntArray) : VectorIndirection {
            override fun valueCount(): Int = idxs.size

            override fun getIndex(idx: Int): Int = idxs[idx]

            override fun select(mask: Mask): VectorIndirection = Selection(select(mask.idxs))

            override fun selectFrom(vec: VectorReader) = vec.select(idxs)

            override fun toString(): String = "(Selection {idxs=${this.idxs.contentToString()}})"

            override operator fun iterator(): VectorIndirection.Iterator =
                object : VectorIndirection.Iterator() {
                    private val idxs = this@Selection.idxs
                    private var idx = 0
                    override fun hasNext() = idx < idxs.size
                    override fun nextInt() = idxs[idx++]

                    override fun seek(idx: Int) {
                        this.idx = idx
                    }
                }

            override fun equals(other: Any?) = when {
                this === other -> true
                other !is Selection -> false
                else -> idxs.contentEquals(other.idxs)
            }

            override fun hashCode() = idxs.contentHashCode()
        }
    }

    /**
     * An immutable on-heap bitmask over [rowCount] rows: row `i` is bit `i % 64` of word `i / 64`.
     */
    class Mask private constructor(private val words: LongArray, val rowCount: Int) : VectorIndirection {

        val cardinality: Int = words.sumOf { bitCount(it) }

        val wordCount: Int get() = words.size

        fun isSet(row: Int): Boolean = (words[row ushr 6] ushr (row and 63)) and 1L != 0L

        /** Rows `64 * w` to `64 * w + 63`, row `64 * w` in the low bit. */
        fun word(w: Int): Long = words[w]

        /** [toSelection], built on first use and shared by every reader of this mask. */
        override val idxs: IntArray by lazy(LazyThreadSafetyMode.PUBLICATION) { toSelection() }

        override fun valueCount() = cardinality

        override fun getIndex(idx: Int) = idxs[idx]

        override fun select(mask: Mask): VectorIndirection = andThen(mask)

        override fun selectFrom(vec: VectorReader) = vec.select(this)

        /** Walks the mask a word at a time; only [Iterator.seek] reads [idxs]. */
        override fun iterator(): VectorIndirection.Iterator =
            object : VectorIndirection.Iterator() {
                private val words = this@Mask.words
                private val cardinality = this@Mask.cardinality
                private var wordIdx = 0

                // the set bits of word `wordIdx` that the reader hasn't reached yet
                private var remainingBits = if (words.isNotEmpty()) words[0] else 0L

                private var returned = 0

                override fun hasNext() = returned < cardinality

                override fun nextInt(): Int {
                    // hasNext() means a set row is still ahead, so skipping empty words stops before the last word runs out
                    while (remainingBits == 0L) remainingBits = words[++wordIdx]
                    returned++
                    val row = (wordIdx shl 6) + numberOfTrailingZeros(remainingBits)
                    // clears the lowest set bit, the row just returned
                    remainingBits = remainingBits and (remainingBits - 1)
                    return row
                }

                override fun seek(idx: Int) {
                    val row = idxs[idx]
                    wordIdx = row ushr 6
                    // `-1L shl n` clears bits 0 until n, so this keeps `row` and the set rows after it in its word
                    remainingBits = words[wordIdx] and (-1L shl (row and 63))
                    returned = idx
                }
            }

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
         * It's how a filter over a filter stays one mask deep, as in [select].
         *
         * [refinement] is over this mask's set rows, so its [rowCount] MUST be this mask's [cardinality]: its bit `k` keeps or drops this mask's `k`th set row.
         * As selections, `andThen(r).toSelection()` is `toSelection()` indexed by `r.toSelection()`, the way two selections compose.
         */
        fun andThen(refinement: Mask): Mask {
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
            return Mask(out, rowCount)
        }

        /**
         * Composition's inverse: the mask over this mask's set rows that keeps what [refined] keeps, so `andThen(extract(refined))` keeps the rows both set.
         * Where this mask picks a view's rows out of an underlying vector, [andThen] takes a mask over the view's rows down to the vector's, and this brings one back.
         *
         * [refined] is over this mask's [rowCount] rows.
         * Bit `k` of the result is its value at this mask's `k`th set row, and rows of [refined] this mask doesn't set are dropped.
         */
        fun extract(refined: Mask): Mask {
            require(refined.rowCount == rowCount) { "refined mask is over ${refined.rowCount} rows, this over $rowCount" }

            val out = LongArray((cardinality + 63) ushr 6)
            var k = 0
            for (w in words.indices) {
                val word = words[w]
                // not only a fast path: once `k` has reached `cardinality`, a trailing empty word's `outWord` is past the end of `out`
                if (word == 0L) continue

                val bits = compress(refined.words[w], word)

                val n = bitCount(word)
                val outWord = k ushr 6
                val shift = k and 63
                out[outWord] = out[outWord] or (bits shl shift)

                // `bits shl shift` drops the top `shift` bits of `bits`; where any of them were in use, they start the next output word.
                // `shift` is non-zero here (`n` is at most 64), so this never shifts by 64, which the JVM takes mod 64 as a shift by 0.
                // The next word has nothing in it yet, so this assigns rather than ORs.
                if (shift + n > 64) out[outWord + 1] = bits ushr (64 - shift)
                k += n
            }
            return Mask(out, cardinality)
        }

        private inline fun combine(other: Mask, op: (Long, Long) -> Long): Mask {
            require(other.rowCount == rowCount) { "masks over $rowCount and ${other.rowCount} rows" }
            return Mask(LongArray(words.size) { op(words[it], other.words[it]) }, rowCount)
        }

        infix fun and(other: Mask): Mask = combine(other) { a, b -> a and b }

        /**
         * Builds a [Mask] over [rowCount] rows from its set rows, added in strictly ascending order.
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

            fun build(): Mask {
                if (curWord >= 0) words[curWord] = cur
                last = BUILT
                return Mask(words, rowCount)
            }

            private companion object {
                const val BUILT = Int.MAX_VALUE
            }
        }

        companion object {
            @JvmStatic
            fun all(rowCount: Int): Mask {
                val words = LongArray((rowCount + 63) ushr 6) { -1L }
                val tailBits = rowCount and 63
                // a partial last word sets only its low `tailBits` bits, the rows below `rowCount`
                if (tailBits != 0) words[words.size - 1] = (1L shl tailBits) - 1
                return Mask(words, rowCount)
            }

            /**
             * Wraps [words] without copying: the caller MUST NOT write to them afterwards.
             *
             * @param words row `i` is bit `i % 64` of word `i / 64`, one word per 64 rows (or part thereof).
             * @throws IllegalArgumentException if there isn't exactly one word per 64 rows, or a bit at or past [rowCount] is set.
             */
            @JvmStatic
            fun ofWords(words: LongArray, rowCount: Int): Mask {
                require(rowCount >= 0) { "negative rowCount $rowCount" }
                require(words.size == (rowCount + 63) ushr 6) { "${words.size} words over $rowCount rows" }

                // the last word's bits from `tailBits` up are past `rowCount`, so shifting the rows below it out has to leave zero
                val tailBits = rowCount and 63
                require(tailBits == 0 || words.last() ushr tailBits == 0L) { "a bit is set at or past row $rowCount" }

                return Mask(words, rowCount)
            }
        }
    }
}
