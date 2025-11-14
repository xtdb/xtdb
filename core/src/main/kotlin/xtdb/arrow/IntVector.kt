package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import xtdb.util.closeOnCatch

class IntVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer
) : IntegerVector(), MetadataFlavour.Number {

    override val arrowType: ArrowType = I32.arrowType
    override val byteWidth = Int.SIZE_BYTES

    companion object {
        private fun openValidityBuffer(al: BufferAllocator, valueCount: Int) =
            BitBuffer(al, valueCount)

        private fun openDataBuffer(al: BufferAllocator, valueCount: Int) =
            ExtensibleBuffer(al, valueCount.toLong() * Int.SIZE_BYTES)

        @JvmStatic
        @JvmOverloads
        fun open(al: BufferAllocator, name: String, nullable: Boolean, valueCount: Int = 0): IntVector =
            openDataBuffer(al, valueCount).closeOnCatch { dataBuffer ->
                val validityBuffer = if (nullable) openValidityBuffer(al, valueCount) else null
                validityBuffer.closeOnCatch {
                    IntVector(al, name, valueCount, validityBuffer, dataBuffer)
                }
            }
    }

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(v: Int) = writeInt0(v)
    override fun getLong(idx: Int) = getInt(idx).toLong()

    override fun getAsInt(idx: Int) = getInt(idx)
    override fun getAsLong(idx: Int) = getInt(idx).toLong()
    override fun getAsFloat(idx: Int) = getInt(idx).toFloat()
    override fun getAsDouble(idx: Int) = getInt(idx).toDouble()

    fun increment(idx: Int, v: Int) {
        ensureCapacity(idx + 1)
        setInt(idx, if (isNull(idx)) v else getInt(idx) + v)
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getInt(idx)

    override fun writeObject0(value: Any) {
        if (value is Int) writeInt(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeInt(v.readInt())

    override fun getMetaDouble(idx: Int) = getInt(idx).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getInt(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        dataBuffer.openSlice(al).closeOnCatch { dataBuffer ->
            val validityBuffer = this.validityBuffer?.openSlice(al)
            validityBuffer.closeOnCatch {
                IntVector(al, name, valueCount, validityBuffer, dataBuffer)
            }
        }
}