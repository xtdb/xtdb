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
import java.time.*
import java.time.ZoneOffset.UTC
import java.time.temporal.ChronoUnit
import java.util.*

private fun TimeUnit.toInstant(value: Long) = when (this) {
    SECOND -> Instant.ofEpochSecond(value)
    MILLISECOND -> Instant.ofEpochMilli(value)
    MICROSECOND -> Instant.EPOCH.plus(value, ChronoUnit.MICROS)
    NANOSECOND -> Instant.EPOCH.plusNanos(value)
}

class TimestampLocalVector private constructor(
    override var name: String, override var nullable: Boolean, val unit: TimeUnit = MICROSECOND,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer,
    override var valueCount: Int
) : FixedWidthVector(), MetadataFlavour.DateTime {

    override val type = ArrowType.Timestamp(unit, null)
    override val byteWidth = Long.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit) :
            this(name, nullable, unit, ExtensibleBuffer(al), ExtensibleBuffer(al), 0)

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(v: Long) = writeLong0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): LocalDateTime =
        LocalDateTime.ofInstant(unit.toInstant(getLong(idx)), UTC)

    override fun writeObject0(value: Any) = writeLong(
        when (value) {
            is LocalDateTime -> unit.toLong(value.toEpochSecond(UTC), value.nano)
            else -> throw InvalidWriteObjectException(fieldType, value)
        }
    )

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
        TimestampLocalVector(name, nullable, unit, validityBuffer.openSlice(al), dataBuffer.openSlice(al), valueCount)
}

class TimestampTzVector private constructor(
    override var name: String, override var nullable: Boolean,
    val unit: TimeUnit = MICROSECOND, val zone: ZoneId = UTC,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer,
    override var valueCount: Int
) : FixedWidthVector(), MetadataFlavour.DateTime {

    override val type = ArrowType.Timestamp(unit, zone.toString())
    override val byteWidth = Long.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit, zone: ZoneId)
            : this(name, nullable, unit, zone, ExtensibleBuffer(al), ExtensibleBuffer(al), 0)

    override fun getLong(idx: Int) = getLong0(idx)

    override fun writeLong(v: Long) = writeLong0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): ZonedDateTime =
        ZonedDateTime.ofInstant(unit.toInstant(getLong(idx)), zone)

    // False positive in Kotlin 2.2.0: bug KT-78352
    @Suppress("IDENTITY_SENSITIVE_OPERATIONS_WITH_VALUE_TYPE")
    override fun writeObject0(value: Any): Unit =
        when (value) {
            is Instant -> value
            is ZonedDateTime ->
                if (value.zone == zone) value.toInstant()
                else throw InvalidWriteObjectException(fieldType, value)

            is OffsetDateTime ->
                if (value.offset == zone) value.toInstant()
                else throw InvalidWriteObjectException(fieldType, value)

            is Date -> value.toInstant()
            else -> throw InvalidWriteObjectException(fieldType, value)
        }.let {
            writeLong(unit.toLong(it.epochSecond, it.nano))
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
        TimestampTzVector(
            name, nullable, unit, zone, validityBuffer.openSlice(al),
            dataBuffer.openSlice(al), valueCount
        )
}