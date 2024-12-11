package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import java.time.*
import java.time.ZoneOffset.UTC
import java.time.temporal.ChronoUnit

private fun TimeUnit.toInstant(value: Long) = when(this) {
    SECOND -> Instant.ofEpochSecond(value)
    MILLISECOND -> Instant.ofEpochMilli(value)
    MICROSECOND -> Instant.EPOCH.plus(value, ChronoUnit.MICROS)
    NANOSECOND -> Instant.EPOCH.plusNanos(value)
}

class TimestampLocalVector(
    al: BufferAllocator,
    override var name: String,
    nullable: Boolean,
    val unit: TimeUnit = MICROSECOND,
) : FixedWidthVector(al, nullable, ArrowType.Timestamp(unit, null), Long.SIZE_BYTES) {

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): LocalDateTime = LocalDateTime.ofInstant(unit.toInstant(getLong(idx)), UTC)

    override fun writeObject0(value: Any) = writeLong(when (value) {
        is LocalDateTime -> unit.toLong(value.toEpochSecond(UTC), value.nano)
        else -> throw InvalidWriteObjectException(fieldType, value)
    })
}

class TimestampTzVector(
    al: BufferAllocator,
    override var name: String,
    nullable: Boolean,
    val unit: TimeUnit = MICROSECOND,
    val zone: ZoneId = UTC
) : FixedWidthVector(al, nullable, ArrowType.Timestamp(unit, zone.toString()), Long.SIZE_BYTES) {

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): ZonedDateTime = ZonedDateTime.ofInstant(unit.toInstant(getLong(idx)), zone)

    override fun writeObject0(value: Any) = writeLong(when (value) {
        is ZonedDateTime -> unit.toLong(value.toEpochSecond(), value.nano)
        is OffsetDateTime -> unit.toLong(value.toEpochSecond(), value.nano)
        is Instant -> unit.toLong(value.epochSecond, value.nano)
        else -> throw InvalidWriteObjectException(fieldType, value)
    })
}
