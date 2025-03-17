package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.DateUnit.DAY
import org.apache.arrow.vector.types.DateUnit.MILLISECOND
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import java.time.Duration
import java.time.LocalDate

class DateDayVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector() {

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val byteWidth = Int.SIZE_BYTES
    override val type = ArrowType.Date(DAY)

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(value: Int) = writeInt0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = LocalDate.ofEpochDay(getInt(idx).toLong())

    override fun writeObject0(value: Any) {
        if (value is LocalDate) writeInt(value.toEpochDay().toInt())
        else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getInt(idx) * MILLIS_PER_DAY * 1_000_000L)

    override fun openSlice(al: BufferAllocator) =
        DateDayVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}

private val MILLIS_PER_DAY = Duration.ofDays(1).toMillis()

class DateMilliVector internal constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: ExtensibleBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector() {

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(name, nullable, 0, ExtensibleBuffer(al), ExtensibleBuffer(al))

    override val byteWidth = Long.SIZE_BYTES
    override val type = ArrowType.Date(MILLISECOND)

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(value: Long) = writeLong0(value)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = LocalDate.ofEpochDay(getLong(idx) / MILLIS_PER_DAY)!!

    override fun writeObject0(value: Any) {
        if (value is LocalDate) writeLong(value.toEpochDay() * MILLIS_PER_DAY)
        else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getInt(idx) * 1_000_000L)

    override fun openSlice(al: BufferAllocator) =
        DateMilliVector(name, nullable, valueCount, validityBuffer.openSlice(al), dataBuffer.openSlice(al))
}
