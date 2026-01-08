package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.agg.VectorSummer
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.time.MICRO_HZ
import xtdb.time.MILLI_HZ
import xtdb.time.NANO_HZ
import xtdb.time.hz
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
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    val unit: TimeUnit = MICROSECOND,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Duration {

    override val byteWidth = Long.SIZE_BYTES
    override val arrowType = ArrowType.Duration(unit)

    constructor(
        al: BufferAllocator, name: String, nullable: Boolean, unit: TimeUnit
    ) : this(al, name, 0, unit, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(v: Long) = writeLong0(v)

    fun increment(idx: Int, v: Long) {
        ensureCapacity(idx + 1)
        setLong(idx, if (isNull(idx)) v else getLong(idx) + v)
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = unit.toDuration(getLong(idx))

    override fun writeObject0(value: Any) = when (value) {
        is Duration -> writeLong(unit.toLong(value.seconds, value.nano))
        else -> throw InvalidWriteObjectException(arrowType, nullable, value)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())

    override fun divideInto(divisorVec: Vector, outVec: Vector): Vector {
        check(divisorVec is IntegerVector) { "Cannot divide DurationVector by ${divisorVec.arrowType}" }
        check(outVec is DurationVector) { "Cannot divide DurationVector into ${outVec.arrowType}" }

        repeat(valueCount) { idx ->
            if (isNull(idx) || divisorVec.isNull(idx)) {
                outVec.writeNull()
            } else {
                val dividend = getLong(idx)
                val divisor = divisorVec.getAsLong(idx)
                if (divisor == 0L) outVec.writeNull() else outVec.writeLong(dividend / divisor)
            }
        }

        return outVec
    }

    override fun sumInto(outVec: Vector): VectorSummer {
        check(outVec is DurationVector) { "Cannot sum DurationVector into ${outVec.arrowType}" }
        check(outVec.unit >= unit) {
            "Cannot sum DurationVector with unit $unit into DurationVector with unit ${outVec.unit}"
        }

        val multiplier = outVec.unit.hz / unit.hz

        return VectorSummer { idx, groupIdx -> outVec.increment(groupIdx, getLong(idx) * multiplier) }
    }

    override fun getMetaDouble(idx: Int) =
        when (unit) {
            SECOND -> getLong(idx).toDouble()
            MILLISECOND -> getLong(idx) / MILLI_HZ.toDouble()
            MICROSECOND -> getLong(idx) / MICRO_HZ.toDouble()
            NANOSECOND -> getLong(idx) / NANO_HZ.toDouble()
        }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getMetaDouble(idx))

    override fun openSlice(al: BufferAllocator) =
        DurationVector(al, name, valueCount, unit, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}