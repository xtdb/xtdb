package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.InternalApi
import xtdb.api.query.IKeyFn
import xtdb.arrow.agg.VectorSummer
import xtdb.util.Hasher
import java.lang.Long.numberOfTrailingZeros
import java.nio.ByteBuffer

/**
 * The rows of [inner] that [mask] sets: logical row `i` is the `i`th set row.
 *
 * [valueReader] walks the mask a word at a time.
 * Access by logical row reads through [RowMask]'s selection, built on the first such access and shared with every other reader of the same mask.
 */
class MaskedVector(private val inner: VectorReader, private val mask: RowMask) : VectorReader, LongLongVectorReader {
    private val rows get() = mask.selection

    private fun selected() = inner.select(rows)

    override val name: String get() = inner.name
    override val valueCount: Int get() = mask.cardinality
    override val nullable: Boolean get() = inner.nullable
    override val arrowType: ArrowType get() = inner.arrowType
    override val field: Field get() = inner.field
    override val childFields: List<Field> get() = inner.childFields
    override val type: VectorType get() = inner.type

    override fun isNull(idx: Int) = inner.isNull(rows[idx])
    override fun getBoolean(idx: Int) = inner.getBoolean(rows[idx])
    override fun getByte(idx: Int) = inner.getByte(rows[idx])
    override fun getShort(idx: Int) = inner.getShort(rows[idx])
    override fun getInt(idx: Int) = inner.getInt(rows[idx])
    override fun getLong(idx: Int) = inner.getLong(rows[idx])
    override fun getFloat(idx: Int) = inner.getFloat(rows[idx])
    override fun getDouble(idx: Int) = inner.getDouble(rows[idx])
    override fun getBytes(idx: Int): ByteBuffer = inner.getBytes(rows[idx])
    override fun getPointer(idx: Int, reuse: ArrowBufPointer) = inner.getPointer(rows[idx], reuse)
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = inner.getObject(rows[idx], keyFn)

    override fun getLongLongHigh(idx: Int) = (inner as LongLongVectorReader).getLongLongHigh(rows[idx])
    override fun getLongLongLow(idx: Int) = (inner as LongLongVectorReader).getLongLongLow(rows[idx])

    override val keyNames get() = inner.keyNames
    override val legNames get() = inner.legNames
    override fun vectorForOrNull(name: String) = inner.vectorForOrNull(name)?.let { MaskedVector(it, mask) }

    override fun getLeg(idx: Int) = inner.getLeg(rows[idx])
    override fun getTypeId(idx: Int) = inner.getTypeId(rows[idx])

    override val listElements get() = inner.listElements
    override fun getListStartIndex(idx: Int) = inner.getListStartIndex(rows[idx])
    override fun getListCount(idx: Int) = inner.getListCount(rows[idx])

    override val mapKeys: VectorReader get() = inner.mapKeys
    override val mapValues: VectorReader get() = inner.mapValues

    override fun hashCode(idx: Int, hasher: Hasher) = inner.hashCode(rows[idx], hasher)
    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) = selected().hashTo(out, hasher, sel)

    @InternalApi
    override fun write(out: PageOutput, startIdx: Int, len: Int) = selected().write(out, startIdx, len)

    override fun openSlice(al: BufferAllocator) = MaskedVector(inner.openSlice(al), mask)

    override fun select(idxs: IntArray) = selected().select(idxs)

    override fun rowCopier(dest: VectorWriter) = selected().rowCopier(dest)

    override fun valueReader(): RowValueReader = MaskedRows(inner.valueReader(), mask)

    override fun sumInto(outVec: Vector): VectorSummer = selected().sumInto(outVec)
    override fun squareInto(outVec: Vector) = selected().squareInto(outVec)
    override fun sqrtInto(outVec: Vector) = selected().sqrtInto(outVec)

    override fun close() = inner.close()
}

private class MaskedRows(private val inner: ValueReader, private val mask: RowMask) : RowValueReader, ValueReader by inner {
    private val cardinality = mask.cardinality
    private val wordCount = mask.wordCount
    private var wordIdx = 0

    // the set bits of word `wordIdx` that the reader hasn't reached yet
    private var remainingBits = if (wordCount > 0) mask.word(0) else 0L

    private var logical = -1

    override var pos: Int
        get() = logical
        set(value) {
            logical = value
            val row = mask.selection[value]
            wordIdx = row ushr 6
            // `-2L shl n` clears bits 0..n, so this keeps the set rows after `row` in its word
            remainingBits = mask.word(wordIdx) and (-2L shl (row and 63))
            inner.pos = row
        }

    override fun hasNext() = logical + 1 < cardinality

    override fun next(): RowValueReader {
        if (!hasNext()) throw NoSuchElementException()
        // hasNext() means a set row is still ahead, so skipping empty words stops before the last word runs out
        while (remainingBits == 0L) remainingBits = mask.word(++wordIdx)
        logical++
        inner.pos = (wordIdx shl 6) + numberOfTrailingZeros(remainingBits)
        // clears the lowest set bit, the row just moved to
        remainingBits = remainingBits and (remainingBits - 1)
        return this
    }
}
