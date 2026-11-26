package xtdb.arrow

import com.carrotsearch.hppc.DoubleArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorIndirection.Mask
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.hash.addDoubles
import xtdb.util.Hasher

class ByteVector private constructor(
    override val al: BufferAllocator,
    override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : IntegerVector(), MetadataFlavour.Number {

    constructor(
        al: BufferAllocator, nullable: Boolean
    ) : this(al, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al))

    override val byteWidth = Byte.SIZE_BYTES
    override val arrowType: ArrowType = I8_TYPE

    override fun getByte(idx: Int) = getByte0(idx)
    override fun writeByte(v: Byte) = writeByte0(v)
    override fun setByte(idx: Int, v: Byte) = setByte0(idx, v)

    override fun getAsInt(idx: Int) = getByte(idx).toInt()
    override fun getAsLong(idx: Int) = getByte(idx).toLong()
    override fun getAsFloat(idx: Int) = getByte(idx).toFloat()
    override fun getAsDouble(idx: Int) = getByte(idx).toDouble()

    override fun compare(op: CmpOp, rhs: Long, mask: Mask) = cmpLongs(op, rhs, mask) { dataBuffer.getByte(it).toLong() }
    override fun compare(op: CmpOp, rhs: Double, mask: Mask) = cmpDoubles(op, rhs, mask) { dataBuffer.getByte(it).toDouble() }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getByte(idx)

    override fun writeObject0(value: Any) {
        if (value is Byte) writeByte(value) else throw InvalidWriteObjectException(this, value)
    }

    override fun writeValue0(v: ValueReader) = writeByte(v.readByte())

    override fun rowCopier0(src: VectorReader): RowCopier {
        check(src is ByteVector)
        return super.rowCopier0(src)
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getByte(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        ByteVector(al, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al))

    override fun doublesTo(out: DoubleArrayList, sel: IntArray?) = out.addDoubles(valueCount, sel) { dataBuffer.getByte(it).toDouble() }
}