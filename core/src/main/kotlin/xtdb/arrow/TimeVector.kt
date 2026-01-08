package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.time.MICRO_HZ
import xtdb.time.MILLI_HZ
import xtdb.time.NANO_HZ
import java.time.LocalTime

internal fun TimeUnit.toInt(value: LocalTime) = when (this) {
    SECOND, MILLISECOND -> toLong(value.toSecondOfDay().toLong(), value.nano).toInt()
    else -> throw UnsupportedOperationException("can't convert to int: $this")
}

internal fun TimeUnit.toLocalTime(value: Int): LocalTime = when (this) {
    SECOND -> LocalTime.ofSecondOfDay(value.toLong())
    MILLISECOND -> LocalTime.ofNanoOfDay(value.toLong() * 1000_000)
    else -> throw UnsupportedOperationException("can't convert from int: $this")
}

internal fun TimeUnit.toLocalTime(value: Long): LocalTime = when (this) {
    SECOND, MILLISECOND -> toLocalTime(value.toInt())
    MICROSECOND -> LocalTime.ofNanoOfDay(value * 1000)
    NANOSECOND -> LocalTime.ofNanoOfDay(value)
}

class Time32Vector private constructor(
    override val al: BufferAllocator,
    override var name: String, val unit: TimeUnit,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer,
    override var valueCount: Int
) : FixedWidthVector(), MetadataFlavour.TimeOfDay {

    override val arrowType = ArrowType.Time(unit, Int.SIZE_BITS)
    override val byteWidth = Int.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit)
            : this(al, name, unit, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al), 0)

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(v: Int) = writeInt0(v)

    override fun writeLong(v: Long) = writeInt(v.toInt())

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toLocalTime(getInt(idx))

    override fun writeObject0(value: Any) {
        if (value is LocalTime) writeInt(unit.toInt(value))
        else throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeInt(v.readInt())

    override fun getMetaDouble(idx: Int) = when(unit) {
        SECOND -> getInt(idx).toDouble()
        MILLISECOND -> getInt(idx) / (MILLI_HZ.toDouble())
        else -> error("invalid unit")
    }

    override fun openSlice(al: BufferAllocator) =
        Time32Vector(al, name, unit, validityBuffer?.openSlice(al), dataBuffer.openSlice(al), valueCount)
}

class Time64Vector private constructor(
    override val al: BufferAllocator,
    override var name: String, private val unit: TimeUnit,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer,
    override var valueCount: Int
) : FixedWidthVector(), MetadataFlavour.TimeOfDay {

    override val arrowType = ArrowType.Time(unit, Long.SIZE_BITS)
    override val byteWidth = Long.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit)
            : this(al, name, unit, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al), 0)

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(v: Long) = writeLong0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toLocalTime(getLong(idx))

    override fun writeObject0(value: Any) {
        if (value is LocalTime) writeLong(unit.toLong(value.toSecondOfDay().toLong(), value.nano))
        else throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())

    override fun getMetaDouble(idx: Int) =
        getLong(idx) / when (unit) {
            MICROSECOND -> MICRO_HZ
            NANOSECOND -> NANO_HZ
            else -> error("unsupported unit")
        }.toDouble()

    override fun openSlice(al: BufferAllocator) =
        Time64Vector(al, name, unit, validityBuffer?.openSlice(al), dataBuffer.openSlice(al), valueCount)
}
