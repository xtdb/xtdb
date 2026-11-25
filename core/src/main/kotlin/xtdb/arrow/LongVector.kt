package xtdb.arrow

import com.carrotsearch.hppc.DoubleArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.hash.addDoubles
import xtdb.util.Hasher

class LongVector private constructor(
    override val al: BufferAllocator,
    override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : IntegerVector(), MetadataFlavour.Number {

    constructor(al: BufferAllocator, nullable: Boolean)
            : this(al, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override val arrowType: ArrowType = MinorType.BIGINT.type
    override val byteWidth = Long.SIZE_BYTES

    override fun getLong(idx: Int) = getLong0(idx)
    override fun writeLong(v: Long) = writeLong0(v)
    override fun setLong(idx: Int, v: Long) = setLong0(idx, v)

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
        if (value is Long) writeLong(value) else throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeLong(v.readLong())

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getLong(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        LongVector(al, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))

    override fun doublesTo(out: DoubleArrayList, sel: IntArray?) = out.addDoubles(valueCount, sel) { dataBuffer.getLong(it).toDouble() }
}