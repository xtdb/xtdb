package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.DateUnit.DAY
import org.apache.arrow.vector.types.DateUnit.MILLISECOND
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.error.Incorrect
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorIndirection.Mask
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.hash.addDoubles
import xtdb.hash.doubleHashes
import xtdb.util.Hasher
import java.time.Duration
import java.time.LocalDate

private val MILLIS_PER_DAY = Duration.ofDays(1).toMillis()
private val SECONDS_PER_DAY = Duration.ofDays(1).toSeconds()

private inline fun <R> LocalDate.inRangeOf(vec: Vector, f: () -> R): R =
    try {
        f()
    } catch (e: ArithmeticException) {
        throw Incorrect(
            "date out of range for ${vec.arrowType}: $this",
            errorCode = "xtdb/date-out-of-range",
            data = mapOf("date" to this),
            cause = e
        )
    }

class DateDayVector private constructor(
    override val al: BufferAllocator,
    override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.DateTime {

    constructor(al: BufferAllocator, nullable: Boolean)
            : this(al, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override val byteWidth = Int.SIZE_BYTES
    override val arrowType = ArrowType.Date(DAY)

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(v: Int) = writeInt0(v)
    override fun setInt(idx: Int, v: Int) = setInt0(idx, v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = LocalDate.ofEpochDay(getInt(idx).toLong())

    override fun writeObject0(value: Any) {
        if (value is LocalDate) writeInt(value.inRangeOf(this) { Math.toIntExact(value.toEpochDay()) })
        else throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeInt(v.readInt())

    override fun getMetaDouble(idx: Int) = (getInt(idx) * SECONDS_PER_DAY).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getMetaDouble(idx))

    override fun openSlice(al: BufferAllocator) =
        DateDayVector(al, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))

    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) =
        doubleHashes(this, out, sel, hasher) { it.addDoubles(valueCount, sel) { idx -> if (isNull(idx)) 0.0 else getMetaDouble(idx) } }

    override fun compare(op: CmpOp, rhs: LocalDate, sel: IntArray?, mask: Mask) =
        cmpLongs(op, rhs.toEpochDay(), sel, mask) { dataBuffer.getInt(it).toLong() }
}

class DateMilliVector internal constructor(
    override val al: BufferAllocator,
    override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.DateTime {

    constructor(al: BufferAllocator, nullable: Boolean)
            : this(al, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override val byteWidth = Long.SIZE_BYTES
    override val arrowType = ArrowType.Date(MILLISECOND)

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(v: Long) = writeLong0(v)
    override fun setLong(idx: Int, v: Long) = setLong0(idx, v)

    private fun getEpochDay(idx: Int) = Math.floorDiv(getLong(idx), MILLIS_PER_DAY)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = LocalDate.ofEpochDay(getEpochDay(idx))!!

    override fun writeObject0(value: Any) {
        if (value is LocalDate) value.inRangeOf(this) { writeLong(Math.multiplyExact(value.toEpochDay(), MILLIS_PER_DAY)) }
        else throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())

    override fun getMetaDouble(idx: Int) = (getEpochDay(idx) * SECONDS_PER_DAY).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getMetaDouble(idx))

    override fun openSlice(al: BufferAllocator) =
        DateMilliVector(al, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))

    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) =
        doubleHashes(this, out, sel, hasher) { it.addDoubles(valueCount, sel) { idx -> if (isNull(idx)) 0.0 else getMetaDouble(idx) } }
}
