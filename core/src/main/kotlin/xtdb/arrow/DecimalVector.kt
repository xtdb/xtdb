package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
import xtdb.arrow.agg.VectorSummer
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.error.Unsupported
import xtdb.util.Hasher
import java.math.BigDecimal

private const val DECIMAL_ERROR_KEY = "xtdb.error/decimal-error"

class DecimalVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer,
    private val decimalType: Decimal
) : NumericVector(), MetadataFlavour.Number {

    companion object {
        private val BIT_WIDTHS = setOf(32, 64, 128, 256)
    }

    val precision = decimalType.precision
    val scale = decimalType.scale
    val bitWidth = decimalType.bitWidth

    init {
        require(bitWidth in BIT_WIDTHS) { "Invalid bit width for DecimalVector: $bitWidth" }
    }

    constructor(al: BufferAllocator, name: String, nullable: Boolean, decimalType: Decimal)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al), decimalType)

    override val byteWidth = (bitWidth / 8)

    override val arrowType: ArrowType = Decimal(precision, scale, bitWidth)

    fun getDecimal(idx: Int) = dataBuffer.readBigDecimal(idx, scale, byteWidth)

    override fun getAsFloat(idx: Int): Float = getDecimal(idx).toFloat()
    override fun getAsDouble(idx: Int): Double = getDecimal(idx).toDouble()

    fun increment(idx: Int, v: BigDecimal) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        val result = if (isNull(idx)) v else getDecimal(idx).add(v)
        dataBuffer.setBigDecimal(idx, result, byteWidth)
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): BigDecimal = getDecimal(idx)

    override fun writeObject0(value: Any) {
        if (value is BigDecimal) {
            if (value.precision() > precision || value.scale() != scale) {
                throw InvalidWriteObjectException(this, value)
            }

            // HACK, we throw unsupported here, but it should likely be dealt with in the EE if an object doesn't fit
            try {
                dataBuffer.writeBigDecimal(value, byteWidth)
            } catch (e: UnsupportedOperationException) {
                throw Unsupported(e.message, DECIMAL_ERROR_KEY, cause = e)
            }
            writeNotNull()
        } else throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override fun sumInto(outVec: Vector): VectorSummer {
        if (outVec !is DecimalVector) return super.sumInto(outVec)

        return VectorSummer { idx, groupIdx ->
            outVec.ensureCapacity(groupIdx + 1)
            outVec.increment(groupIdx, if (isNull(idx)) BigDecimal.ZERO else getDecimal(idx))
        }
    }

    override fun getMetaDouble(idx: Int): Double = getObject0(idx, KEBAB_CASE_KEYWORD).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getObject0(idx, KEBAB_CASE_KEYWORD).toDouble())

    override fun openSlice(al: BufferAllocator) =
        DecimalVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al), decimalType)
}