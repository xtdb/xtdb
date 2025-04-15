package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.types.Interval
import java.nio.ByteBuffer
import java.nio.ByteOrder

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

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = Interval.Month(getInt(idx))

    override fun writeObject0(value: Any) =
        if (value is Interval.Month) writeInt(value.months)
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

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Interval.DayTime {
        val buf = getBytes0(idx).duplicate().order(ByteOrder.LITTLE_ENDIAN)
        return Interval.DayTime(buf.getInt(), buf.getInt())
    }

    // Java Arrow uses little endian byte order in underlying BasedFixedWidthVector
    private val buf: ByteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)

    override fun writeObject0(value: Any) =
        if (value is Interval.DayTime) {
            buf.run {
                clear()
                putInt(value.days)
                putInt(value.millis)
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

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Interval.MonthDayNano {
        val buf = getBytes0(idx).duplicate().order(ByteOrder.LITTLE_ENDIAN)
        return Interval.MonthDayNano(buf.getInt(), buf.getInt(), buf.getLong())
    }

    // Java Arrow uses little endian byte order in underlying BasedFixedWidthVector
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
            is Interval.MonthDayNano -> writeObject0(value.months, value.days, value.nanos)
            is Interval.MonthDayMicro -> writeObject0(value.months, value.days, value.nanos)
            else -> throw InvalidWriteObjectException(fieldType, value)
        }

    override val metadataFlavours get() = listOf(this)

    override fun openSlice(al: BufferAllocator) =
        IntervalMonthDayNanoVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}