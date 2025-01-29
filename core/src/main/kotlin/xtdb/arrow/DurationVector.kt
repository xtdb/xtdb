package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import java.time.Duration
import java.time.temporal.ChronoUnit

internal fun TimeUnit.toDuration(value: Long): Duration = when (this) {
    SECOND -> Duration.ofSeconds(value)
    MILLISECOND -> Duration.ofMillis(value)
    MICROSECOND -> Duration.of(value, ChronoUnit.MICROS)
    NANOSECOND -> Duration.ofNanos(value)
}

class DurationVector(
    allocator: BufferAllocator,
    override var name: String,
    nullable: Boolean,
    val unit: TimeUnit = MICROSECOND
) : FixedWidthVector(allocator, nullable, ArrowType.Duration(unit), Long.SIZE_BYTES) {

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toDuration(getLong(idx))

    override fun writeObject0(value: Any) = when (value) {
        is Duration -> writeLong(unit.toLong(value.seconds, value.nano))
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getLong(idx))
}