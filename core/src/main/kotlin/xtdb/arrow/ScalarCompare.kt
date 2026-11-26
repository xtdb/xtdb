package xtdb.arrow

import xtdb.api.error.Fault
import xtdb.arrow.VectorIndirection.Mask

enum class CmpOp { LT, LTE, GT, GTE, EQ, NEQ }

internal fun requireMaskWithin(valueCount: Int, sel: IntArray?, mask: Mask) {
    val n = mask.rowCount
    if (sel == null) {
        if (n > valueCount) throw Fault("mask of $n rows over $valueCount rows", "xtdb/mask-out-of-bounds")
    } else {
        if (n > sel.size) throw Fault("mask of $n rows over a selection of ${sel.size}", "xtdb/mask-out-of-bounds")
    }
}

internal fun BitBuffer?.validWord(w: Int, n: Int, sel: IntArray?): Long {
    if (this == null) return -1L
    // row i is validity bit i, so validity word `w` lines up with mask word `w`, bits past `n` included - the caller's `and` with the mask clears those
    if (sel == null) return wordAt(w)

    val base = w shl 6
    var valid = 0L
    for (b in 0 until minOf(64, n - base)) if (getBoolean(sel[base + b])) valid = valid or (1L shl b)
    return valid
}

internal inline fun evalWords(validity: BitBuffer?, valueCount: Int, sel: IntArray?, mask: Mask, test: (Int) -> Boolean): Mask {
    requireMaskWithin(valueCount, sel, mask)

    val n = mask.rowCount
    val out = LongArray(mask.wordCount)

    for (w in out.indices) {
        val m = mask.word(w)
        if (m == 0L) continue

        val base = w shl 6
        val len = minOf(64, n - base)
        var bits = 0L

        // `test` sees every row of the word, unset and null rows alike, with no branch on either - the final `and` drops their answers
        if (sel == null) {
            for (b in 0 until len) bits = bits or ((if (test(base + b)) 1L else 0L) shl b)
        } else {
            for (b in 0 until len) bits = bits or ((if (test(sel[base + b])) 1L else 0L) shl b)
        }

        out[w] = bits and m and validity.validWord(w, n, sel)
    }

    return Mask.ofWords(out, n)
}

internal inline fun FixedWidthVector.cmpLongs(
    op: CmpOp, rhs: Long, sel: IntArray?, mask: Mask, crossinline read: (Int) -> Long
): Mask = when (op) {
    CmpOp.LT -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) < rhs }
    CmpOp.LTE -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) <= rhs }
    CmpOp.GT -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) > rhs }
    CmpOp.GTE -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) >= rhs }
    CmpOp.EQ -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) == rhs }
    CmpOp.NEQ -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) != rhs }
}

internal inline fun FixedWidthVector.cmpDoubles(
    op: CmpOp, rhs: Double, sel: IntArray?, mask: Mask, crossinline read: (Int) -> Double
): Mask = when (op) {
    CmpOp.LT -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) < rhs }
    CmpOp.LTE -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) <= rhs }
    CmpOp.GT -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) > rhs }
    CmpOp.GTE -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) >= rhs }
    CmpOp.EQ -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) == rhs }
    CmpOp.NEQ -> evalWords(validityBuffer, valueCount, sel, mask) { read(it) != rhs }
}

internal fun notNullBits(validity: BitBuffer?, valueCount: Int, sel: IntArray?, mask: Mask): Mask =
    evalWords(validity, valueCount, sel, mask) { true }
