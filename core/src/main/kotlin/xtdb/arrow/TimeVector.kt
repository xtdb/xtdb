package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
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
    override var name: String, override var nullable: Boolean, val unit: TimeUnit,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer,
    override var valueCount: Int
) : FixedWidthVector() {

    override val type = ArrowType.Time(unit, Int.SIZE_BITS)
    override val byteWidth = Int.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit)
            : this(name, nullable, unit, ExtensibleBuffer(al), ExtensibleBuffer(al), 0)

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toLocalTime(getInt(idx))

    override fun writeObject0(value: Any) {
        if (value is LocalTime) writeInt(unit.toInt(value)) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun openSlice(al: BufferAllocator) =
        Time32Vector(name, nullable, unit, validityBuffer.openSlice(al), dataBuffer.openSlice(al), valueCount)
}

class Time64Vector private constructor(
    override var name: String, override var nullable: Boolean, private val unit: TimeUnit,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer,
    override var valueCount: Int
) : FixedWidthVector() {

    override val type = ArrowType.Time(unit, Long.SIZE_BITS)
    override val byteWidth = Long.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit)
            : this(name, nullable, unit, ExtensibleBuffer(al), ExtensibleBuffer(al), 0)

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toLocalTime(getLong(idx))

    override fun writeObject0(value: Any) {
        if (value is LocalTime) writeLong(unit.toLong(value.toSecondOfDay().toLong(), value.nano))
        else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun openSlice(al: BufferAllocator) =
        Time64Vector(name, nullable, unit, validityBuffer.openSlice(al), dataBuffer.openSlice(al), valueCount)
}
