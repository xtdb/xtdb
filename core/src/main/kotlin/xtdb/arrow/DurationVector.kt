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

class DurationVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    val unit: TimeUnit = MICROSECOND,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector() {

    override val byteWidth = Long.SIZE_BYTES
    override val type = ArrowType.Duration(unit)

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit
    ) : this(name, nullable, 0, unit, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toDuration(getLong(idx))

    override fun writeObject0(value: Any) = when (value) {
        is Duration -> writeLong(unit.toLong(value.seconds, value.nano))
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getLong(idx))
}