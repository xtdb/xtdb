package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.types.IntervalDayTime
import xtdb.types.IntervalMonthDayNano
import xtdb.types.IntervalYearMonth
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Period

class IntervalYearMonthVector(
    al: BufferAllocator,
    override val name: String,
    override var nullable: Boolean
) : FixedWidthVector(al, Int.SIZE_BYTES) {

    override val arrowType: ArrowType = MinorType.INTERVALYEAR.type

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int) = IntervalYearMonth(Period.ofMonths(getInt(idx)))

    override fun writeObject0(value: Any) =
        if (value is IntervalYearMonth) writeInt(value.period.toTotalMonths().toInt())
        else TODO("unknown type: ${value::class.simpleName}")
}

class IntervalDayTimeVector(
    al: BufferAllocator,
    override val name: String,
    override var nullable: Boolean
) : FixedWidthVector(al, Long.SIZE_BYTES) {

    override val arrowType: ArrowType = MinorType.INTERVALDAY.type

    override fun getObject0(idx: Int) =
        IntervalDayTime(
            Period.ofDays(getInt0(idx * 2)),
            Duration.ofMillis(getInt0(idx * 2 + 1).toLong())
        )

    private val buf: ByteBuffer = ByteBuffer.allocate(8)

    override fun writeObject0(value: Any) =
        if (value is IntervalMonthDayNano) {
            require(value.period.toTotalMonths() == 0L) { "non-zero months in DayTime interval" }
            buf.clear()
            buf.putInt(value.period.days)
            buf.putInt(value.duration.toMillis().toInt())
            writeBytes(buf.array())
        } else TODO("unknown type: ${value::class.simpleName}")
}

class IntervalMonthDayNanoVector(
    al: BufferAllocator,
    override val name: String,
    override var nullable: Boolean
) : FixedWidthVector(al, 16) {

    override val arrowType: ArrowType = MinorType.INTERVALMONTHDAYNANO.type

    override fun getObject0(idx: Int) =
        IntervalMonthDayNano(
            Period.of(0, getInt0(idx * 4), getInt0(idx * 4 + 1)),
            Duration.ofNanos(getLong0(idx * 2 + 1))
        )

    private val buf: ByteBuffer = ByteBuffer.allocate(16)

    override fun writeObject0(value: Any) =
        if (value is IntervalMonthDayNano) {
            buf.clear()
            buf.putInt(value.period.toTotalMonths().toInt())
            buf.putInt(value.period.days)
            buf.putLong(value.duration.toNanos())
            writeBytes(buf.array())
        } else TODO("unknown type: ${value::class.simpleName}")
}