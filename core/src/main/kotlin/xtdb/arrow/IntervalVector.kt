package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.PeriodDuration
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.time.Interval
import xtdb.time.MILLI_HZ
import xtdb.time.NANO_HZ
import xtdb.util.Hasher
import xtdb.vector.extensions.IntervalMDMType
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Duration

private class IntervalValueReader(private val vec: VectorReader) : ValueReader {
    override var pos = 0
    override val isNull: Boolean get() = vec.isNull(pos)

    override fun readObject(): Any? =
        vec.getObject(pos)
            ?.let { it as Interval }
            ?.let { PeriodDuration(it.period, it.duration) }
}

class IntervalYearMonthVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Presence {

    override val arrowType: ArrowType = MinorType.INTERVALYEAR.type
    override val byteWidth = Int.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(v: Int) = writeInt0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = Interval(getInt(idx), 0, 0)

    override fun writeObject0(value: Any) {
        when (value) {
            is PeriodDuration if (value.period.days == 0 && value.duration.equals(Duration.ZERO)) -> {
                writeInt(value.period.toTotalMonths().toInt())
            }

            is Interval if (value.days == 0 && value.nanos == 0L) -> writeInt(value.months)
            else -> throw InvalidWriteObjectException(arrowType, nullable, value)
        }
    }

    override fun valueReader(): ValueReader = IntervalValueReader(this)

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        IntervalYearMonthVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}

private const val NANOS_PER_MILLI = NANO_HZ / MILLI_HZ

class IntervalDayTimeVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Presence {

    override val arrowType: ArrowType = MinorType.INTERVALDAY.type
    override val byteWidth = Long.SIZE_BYTES

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean
    ) : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Interval {
        val buf = getBytes0(idx).duplicate().order(ByteOrder.LITTLE_ENDIAN)
        return Interval(0, buf.getInt(), buf.getInt().toLong() * NANOS_PER_MILLI)
    }

    // Java Type uses little endian byte order in underlying BasedFixedWidthVector
    private val buf: ByteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)

    override fun writeObject0(value: Any) =
        when (value) {
            is Interval if (value.months == 0 && value.nanos % NANOS_PER_MILLI == 0L) -> {
                buf.run {
                    clear()
                    putInt(value.days)
                    putInt((value.nanos / NANOS_PER_MILLI).toInt())
                    flip()
                    writeBytes(this)
                }
            }

            is PeriodDuration if (value.period.toTotalMonths() == 0L && value.duration.toNanos() % NANOS_PER_MILLI == 0L) -> {
                buf.run {
                    clear()
                    putInt(value.period.days)
                    putInt((value.duration.toNanos() / NANOS_PER_MILLI).toInt())
                    flip()
                    writeBytes(this)
                }
            }

            else -> throw InvalidWriteObjectException(arrowType, nullable, value)
        }

    override fun valueReader(): ValueReader = IntervalValueReader(this)

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        IntervalDayTimeVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}

class IntervalMonthDayNanoVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Presence {

    override val arrowType: ArrowType = MinorType.INTERVALMONTHDAYNANO.type
    override val byteWidth = 16

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Interval {
        val buf = getBytes0(idx).duplicate().order(ByteOrder.LITTLE_ENDIAN)
        return Interval(buf.getInt(), buf.getInt(), buf.getLong())
    }

    // Java Type uses little endian byte order in underlying BasedFixedWidthVector
    private val buf: ByteBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)

    private fun writeObject0(months: Int, days: Int, nanos: Long) {
        buf.run {
            clear()
            putInt(months)
            putInt(days)
            putLong(nanos)
            flip()
            writeBytes(this)
        }
    }

    override fun writeObject0(value: Any) =
        when (value) {
            is Interval -> writeObject0(value.months, value.days, value.nanos)

            is PeriodDuration -> {
                try {
                    writeObject0(
                        value.period.toTotalMonths().toInt(),
                        value.period.days,
                        value.duration.toNanos()
                    )
                } catch (_: ArithmeticException) {
                    // we normalise iff the user gives us a Duration that's too big to fit in a long
                    val extraDays = value.duration.toDays()
                    val dur = value.duration.minusDays(extraDays)
                    writeObject0(
                        value.period.toTotalMonths().toInt(),
                        (value.period.days + extraDays).toInt(),
                        dur.toNanos()
                    )
                }
            }

            else -> throw InvalidWriteObjectException(arrowType, nullable, value)
        }

    override fun valueReader(): ValueReader = IntervalValueReader(this)

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        IntervalMonthDayNanoVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}

class IntervalMonthDayMicroVector(
    override val inner: IntervalMonthDayNanoVector
) : ExtensionVector(), MetadataFlavour.Presence {

    override val arrowType = IntervalMDMType

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = inner.getObject0(idx, keyFn)
    override fun writeObject0(value: Any) = inner.writeObject(value)

    override val metadataFlavours get() = listOf(this)

    override fun hashCode0(idx: Int, hasher: Hasher) = inner.hashCode0(idx, hasher)

    override fun openSlice(al: BufferAllocator) = IntervalMonthDayMicroVector(inner.openSlice(al))
}