package xtdb.hash

import com.carrotsearch.hppc.DoubleArrayList
import org.apache.arrow.memory.util.ArrowBufPointer.NULL_HASH_CODE
import xtdb.arrow.*
import xtdb.util.Hasher

internal fun perRowHashes(vec: VectorReader, out: IntVector, sel: IntArray?, hasher: Hasher) {
    out.writeInts(sel?.size ?: vec.valueCount) { i -> vec.hashCode(if (sel == null) i else sel[i], hasher) }
}

private inline fun IntVector.writeHashes(
    validity: BitBuffer?, valueCount: Int, sel: IntArray?, hash: (idx: Int) -> Int
) = writeInts(sel?.size ?: valueCount) { i ->
    val idx = if (sel == null) i else sel[i]
    if (validity != null && !validity.getBoolean(idx)) NULL_HASH_CODE else hash(idx)
}

/**
 * For the fixed-width types that keep [FixedWidthVector]'s default `hashCode0`, which hashes each row's bytes.
 */
internal fun rawBytesHashes(vec: FixedWidthVector, out: IntVector, sel: IntArray?, hasher: Hasher) {
    val data = vec.dataBuffer
    val width = vec.byteWidth
    out.writeHashes(vec.validityBuffer, vec.valueCount, sel) { data.hashCode(hasher, it * width, width) }
}

internal fun variableWidthHashes(vec: VariableWidthVector, out: IntVector, sel: IntArray?, hasher: Hasher) {
    val offsets = vec.offsetBuffer
    val data = vec.dataBuffer

    out.writeHashes(vec.validityBuffer, vec.valueCount, sel) {
        val start = offsets.getInt(it)
        data.hashCode(hasher, start, offsets.getInt(it + 1) - start)
    }
}

internal inline fun DoubleArrayList.addDoubles(valueCount: Int, sel: IntArray?, get: (idx: Int) -> Double) {
    val n = sel?.size ?: valueCount
    ensureCapacity(elementsCount + n)
    for (i in 0 until n) add(get(if (sel == null) i else sel[i]))
}

internal fun IntVector.writeDoubleHashes(validity: BitBuffer?, sel: IntArray?, doubles: DoubleArrayList, hasher: Hasher) {
    val values = doubles.buffer
    writeInts(doubles.size()) { i ->
        val idx = if (sel == null) i else sel[i]
        if (validity != null && !validity.getBoolean(idx)) NULL_HASH_CODE else hasher.hash(values[i])
    }
}

/**
 * For the types whose `hashCode0` is `hasher.hash(d)` of a per-row double `d`.
 * [doublesTo] appends one double per row of [sel] (else of [vec]); a null row's double is ignored.
 */
internal inline fun doubleHashes(
    vec: FixedWidthVector, out: IntVector, sel: IntArray?, hasher: Hasher, doublesTo: (DoubleArrayList) -> Unit
) {
    val doubles = DoubleArrayList(sel?.size ?: vec.valueCount).also(doublesTo)
    out.writeDoubleHashes(vec.validityBuffer, sel, doubles, hasher)
}
