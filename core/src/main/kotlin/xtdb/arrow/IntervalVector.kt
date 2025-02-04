package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import xtdb.api.query.IKeyFn
import xtdb.types.IntervalDayTime
import xtdb.types.IntervalMonthDayNano
import xtdb.types.IntervalYearMonth
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Duration
import java.time.Period

class IntervalYearMonthVector(
    al: BufferAllocator,
    override var name: String,
    nullable: Boolean
) : FixedWidthVector(al, nullable, MinorType.INTERVALYEAR.type, Int.SIZE_BYTES) {

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = IntervalYearMonth(Period.ofMonths(getInt(idx)))

    override fun writeObject0(value: Any) =
        if (value is IntervalYearMonth) writeInt(value.period.toTotalMonths().toInt())
        else throw InvalidWriteObjectException(fieldType, value)
}

class IntervalDayTimeVector(
    al: BufferAllocator,
    override var name: String,
    nullable: Boolean
) : FixedWidthVector(al, nullable, MinorType.INTERVALDAY.type, Long.SIZE_BYTES) {

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
}

class IntervalMonthDayNanoVector(
    al: BufferAllocator,
    override var name: String,
    nullable: Boolean
) : FixedWidthVector(al, nullable, MinorType.INTERVALMONTHDAYNANO.type, 16) {

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
        if (value is IntervalMonthDayNano) {
            buf.run {
                clear()
                putInt(value.period.toTotalMonths().toInt())
                putInt(value.period.days)
                putLong(value.duration.toNanos())
                flip()
                writeBytes(this)
            }
        } else throw InvalidWriteObjectException(fieldType, value)
}