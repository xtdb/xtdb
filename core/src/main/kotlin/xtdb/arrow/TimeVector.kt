package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import java.time.LocalTime

sealed class TimeVector(allocator: BufferAllocator, nullable: Boolean, arrowType: ArrowType, byteWidth: Int) :
    FixedWidthVector(allocator, nullable, arrowType, byteWidth) {

    abstract val unit: TimeUnit

    abstract override fun getObject0(idx: Int, keyFn: IKeyFn<*>): LocalTime
}

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

class Time32Vector(
    allocator: BufferAllocator,
    override var name: String,
    nullable: Boolean,
    override val unit: TimeUnit
) : TimeVector(allocator, nullable, ArrowType.Time(unit, Int.SIZE_BITS), Int.SIZE_BYTES) {

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toLocalTime(getInt(idx))

    override fun writeObject0(value: Any) {
        if (value is LocalTime) writeInt(unit.toInt(value)) else TODO("not a LocalTime")
    }
}

class Time64Vector(
    allocator: BufferAllocator,
    override var name: String,
    nullable: Boolean,
    override val unit: TimeUnit
) : TimeVector(allocator, nullable, ArrowType.Time(unit, Long.SIZE_BITS), Long.SIZE_BYTES) {

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toLocalTime(getLong(idx))

    override fun writeObject0(value: Any) {
        if (value is LocalTime) writeLong(
            unit.toLong(
                value.toSecondOfDay().toLong(),
                value.nano
            )
        ) else TODO("not a LocalTime")
    }
}
