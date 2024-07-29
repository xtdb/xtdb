package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.DateUnit.*
import org.apache.arrow.vector.types.pojo.ArrowType
import java.time.Duration
import java.time.LocalDate

class DateDayVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean,
) : FixedWidthVector(allocator, Int.SIZE_BYTES) {
    override val arrowType = ArrowType.Date(DAY)

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int) = LocalDate.ofEpochDay(getInt(idx).toLong())!!

    override fun writeObject0(value: Any) {
        if (value is LocalDate) writeInt(value.toEpochDay().toInt()) else TODO("not a LocalDate")
    }
}

private val MILLIS_PER_DAY = Duration.ofDays(1).toMillis()

class DateMilliVector(
    allocator: BufferAllocator,
    override val name: String,
    override var nullable: Boolean,
) : FixedWidthVector(allocator, Long.SIZE_BYTES) {
    override val arrowType = ArrowType.Date(MILLISECOND)

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int) = LocalDate.ofEpochDay(getLong(idx) / MILLIS_PER_DAY)!!

    override fun writeObject0(value: Any) {
        if (value is LocalDate) writeLong(value.toEpochDay() * MILLIS_PER_DAY) else TODO("not a LocalDate")
    }
}
