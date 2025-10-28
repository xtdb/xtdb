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
    override val validityBuffer: BitBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Duration {

    override val byteWidth = Long.SIZE_BYTES
    override val type = ArrowType.Duration(unit)

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit
    ) : this(name, nullable, 0, unit, BitBuffer(al), ExtensibleBuffer(al))

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(v: Long) = writeLong0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toDuration(getLong(idx))

    override fun writeObject0(value: Any) = when (value) {
        is Duration -> writeLong(unit.toLong(value.seconds, value.nano))
        else -> throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())

    override fun getMetaDouble(idx: Int) =
        when (unit) {
            SECOND -> getLong(idx).toDouble()
            MILLISECOND -> getLong(idx) / MILLI_HZ.toDouble()
            MICROSECOND -> getLong(idx) / MICRO_HZ.toDouble()
            NANOSECOND -> getLong(idx) / NANO_HZ.toDouble()
        }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getMetaDouble(idx))

    override fun openSlice(al: BufferAllocator) =
        DurationVector(name, nullable, valueCount, unit, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}