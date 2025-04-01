package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.types.IntervalDayTime
import xtdb.types.IntervalMonthDayNano
import xtdb.types.IntervalYearMonth
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Duration
import java.time.Period

class IntervalYearMonthVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Presence {

    override val type: ArrowType = MinorType.INTERVALYEAR.type
    override val byteWidth = Int.SIZE_BYTES

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(v: Int) = writeInt0(v)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = IntervalYearMonth(Period.ofMonths(getInt(idx)))

    override fun writeObject0(value: Any) =
        if (value is IntervalYearMonth) writeInt(value.period.toTotalMonths().toInt())
        else throw InvalidWriteObjectException(fieldType, value)

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        IntervalYearMonthVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}

class IntervalDayTimeVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Presence {

    override val type: ArrowType = MinorType.INTERVALDAY.type
    override val byteWidth = Long.SIZE_BYTES

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean
    ) : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): IntervalDayTime {
        val buf = getBytes0(idx).duplicate().order(ByteOrder.LITTLE_ENDIAN)
        return IntervalDayTime(
            Period.ofDays(buf.getInt()),
            Duration.ofMillis(buf.getInt().toLong())
        )
    }

    // Java Arrow uses little endian byte order in underlying BasedFixedWidthVector
    private val buf: ByteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)

    override fun writeObject0(value: Any) =
        if (value is IntervalDayTime) {
            require(value.period.toTotalMonths() == 0L) { "non-zero months in DayTime interval" }
            buf.run {
                clear()
                putInt(value.period.days)
                putInt(value.duration.toMillis().toInt())
                flip()
                writeBytes(this)
            }
        } else throw InvalidWriteObjectException(fieldType, value)

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        IntervalDayTimeVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}

class IntervalMonthDayNanoVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Presence {

    override val type: ArrowType = MinorType.INTERVALMONTHDAYNANO.type
    override val byteWidth = 16

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): IntervalMonthDayNano {
        val buf = getBytes0(idx).duplicate().order(ByteOrder.LITTLE_ENDIAN)
        return IntervalMonthDayNano(
            Period.of(0, buf.getInt(), buf.getInt()),
            Duration.ofNanos(buf.getLong())
        )
    }

    // Java Arrow uses little endian byte order in underlying BasedFixedWidthVector
    private val buf: ByteBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)

    override fun writeObject0(value: Any) =
        if (value !is IntervalMonthDayNano) throw InvalidWriteObjectException(fieldType, value)
        else
            buf.run {
                clear()
                putInt(value.period.toTotalMonths().toInt())
                putInt(value.period.days)
                putLong(value.duration.toNanos())
                flip()
                writeBytes(this)
            }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        IntervalMonthDayNanoVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}