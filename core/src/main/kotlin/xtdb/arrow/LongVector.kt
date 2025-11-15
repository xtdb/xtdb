package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class LongVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : IntegerVector(), MetadataFlavour.Number {

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override val arrowType: ArrowType = MinorType.BIGINT.type
    override val byteWidth = Long.SIZE_BYTES

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(v: Long) = writeLong0(v)

    override fun getAsInt(idx: Int) = getLong(idx).toInt()
    override fun getAsLong(idx: Int) = getLong(idx)
    override fun getAsFloat(idx: Int) = getLong(idx).toFloat()
    override fun getAsDouble(idx: Int) = getLong(idx).toDouble()

    fun increment(idx: Int, v: Long) {
        ensureCapacity(idx + 1)
        setLong(idx, if (isNull(idx)) v else getLong(idx) + v)
    }

    override fun squareInto(outVec: Vector): Vector {
        check(outVec is DoubleVector) { "Cannot square LongVector into ${outVec.arrowType}" }
        repeat(valueCount) { idx ->
            if (isNull(idx)) outVec.writeNull() else getAsDouble(idx).let { outVec.writeDouble(it * it) }
        }

        return outVec
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getLong(idx)

    override fun writeObject0(value: Any) {
        if (value is Long) writeLong(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())

    override fun getMetaDouble(idx: Int) = getLong(idx).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getLong(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        LongVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}