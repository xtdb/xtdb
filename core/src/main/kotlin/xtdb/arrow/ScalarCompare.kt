package xtdb.arrow

import xtdb.api.error.Fault
import xtdb.arrow.VectorIndirection.Mask

enum class CmpOp { LT, LTE, GT, GTE, EQ, NEQ }

internal fun requireMaskWithin(valueCount: Int, mask: Mask) {
    val n = mask.rowCount
    if (n > valueCount) throw Fault("mask of $n rows over $valueCount rows", "xtdb/mask-out-of-bounds")
}

internal fun BitBuffer?.validWord(w: Int): Long {
    if (this == null) return -1L
    // row i is validity bit i, so validity word `w` lines up with mask word `w`, bits past the mask's rows included - the caller's `and` with the mask clears those
    return wordAt(w)
}

internal inline fun evalWords(validity: BitBuffer?, valueCount: Int, mask: Mask, test: (Int) -> Boolean): Mask {
    requireMaskWithin(valueCount, mask)

    val n = mask.rowCount
    val out = LongArray(mask.wordCount)

    for (w in out.indices) {
        val m = mask.word(w)
        if (m == 0L) continue

        val base = w shl 6
        val len = minOf(64, n - base)
        var bits = 0L

        // `test` sees every row of the word, unset and null rows alike, with no branch on either - the final `and` drops their answers
        for (b in 0 until len) bits = bits or ((if (test(base + b)) 1L else 0L) shl b)

        out[w] = bits and m and validity.validWord(w)
    }

    return Mask.ofWords(out, n)
}

internal inline fun FixedWidthVector.cmpLongs(
    op: CmpOp, rhs: Long, mask: Mask, crossinline read: (Int) -> Long
): Mask = when (op) {
    CmpOp.LT -> evalWords(validityBuffer, valueCount, mask) { read(it) < rhs }
    CmpOp.LTE -> evalWords(validityBuffer, valueCount, mask) { read(it) <= rhs }
    CmpOp.GT -> evalWords(validityBuffer, valueCount, mask) { read(it) > rhs }
    CmpOp.GTE -> evalWords(validityBuffer, valueCount, mask) { read(it) >= rhs }
    CmpOp.EQ -> evalWords(validityBuffer, valueCount, mask) { read(it) == rhs }
    CmpOp.NEQ -> evalWords(validityBuffer, valueCount, mask) { read(it) != rhs }
}

internal inline fun FixedWidthVector.cmpDoubles(
    op: CmpOp, rhs: Double, mask: Mask, crossinline read: (Int) -> Double
): Mask = when (op) {
    CmpOp.LT -> evalWords(validityBuffer, valueCount, mask) { read(it) < rhs }
    CmpOp.LTE -> evalWords(validityBuffer, valueCount, mask) { read(it) <= rhs }
    CmpOp.GT -> evalWords(validityBuffer, valueCount, mask) { read(it) > rhs }
    CmpOp.GTE -> evalWords(validityBuffer, valueCount, mask) { read(it) >= rhs }
    CmpOp.EQ -> evalWords(validityBuffer, valueCount, mask) { read(it) == rhs }
    CmpOp.NEQ -> evalWords(validityBuffer, valueCount, mask) { read(it) != rhs }
}
