package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class DoubleVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : NumericVector(), MetadataFlavour.Number {

    constructor(al: BufferAllocator, name: String, nullable: Boolean)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override val byteWidth = Double.SIZE_BYTES
    override val arrowType: ArrowType = F64_TYPE

    override fun getDouble(idx: Int) = getDouble0(idx)
    override fun writeDouble(v: Double) = writeDouble0(v)

    override fun getAsFloat(idx: Int) = getDouble(idx).toFloat()
    override fun getAsDouble(idx: Int) = getDouble(idx)

    fun increment(idx: Int, v: Double) {
        ensureCapacity(idx + 1)
        setDouble(idx, if (isNull(idx)) v else getDouble(idx) + v)
    }

    override fun divideInto(divisorVec: Vector, outVec: Vector): Vector {
        check(divisorVec is NumericVector) { "Cannot divide DoubleVector by ${divisorVec.arrowType}" }
        check(outVec is DoubleVector) { "Cannot divide DoubleVector into ${outVec.arrowType}" }

        repeat(valueCount) { idx ->
            if (isNull(idx) || divisorVec.isNull(idx)) {
                outVec.writeNull()
            } else {
                val dividend = getDouble(idx)
                val divisor = divisorVec.getAsDouble(idx)
                if (divisor == 0.0) outVec.writeNull() else outVec.writeDouble(dividend / divisor)
            }
        }

        return outVec
    }

    override fun squareInto(outVec: Vector): MonoVector {
        check(outVec is DoubleVector) { "Cannot square DoubleVector into ${outVec.arrowType}" }
        repeat(valueCount) { idx ->
            if (isNull(idx)) {
                outVec.writeNull()
            } else {
                val value = getDouble(idx)
                outVec.writeDouble(value * value)
            }
        }

        return outVec
    }

    override fun sqrtInto(outVec: Vector): Vector {
        check(outVec is DoubleVector) { "Cannot sqrt DoubleVector into ${outVec.arrowType}" }
        repeat(valueCount) { idx ->
            if (isNull(idx)) {
                outVec.writeNull()
            } else {
                val value = getDouble(idx)
                outVec.writeDouble(kotlin.math.sqrt(value))
            }
        }

        return outVec
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getDouble(idx)

    override fun writeObject0(value: Any) {
        if (value is Double) writeDouble(value) else throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeDouble(v.readDouble())

    override fun getMetaDouble(idx: Int) = getDouble(idx)

    override fun hashCode0(idx: Int, hasher: Hasher): Int = hasher.hash(getDouble(idx))

    override fun openSlice(al: BufferAllocator) =
        DoubleVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))
}