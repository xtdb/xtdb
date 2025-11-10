package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BitVectorHelper
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import xtdb.util.closeOnCatch

internal val I32 = MinorType.INT.type

class IntVector private constructor(
    override var name: String, override var nullable: Boolean, override var valueCount: Int,
    override val validityBuffer: BitBuffer, override val dataBuffer: ExtensibleBuffer
) : FixedWidthVector(), MetadataFlavour.Number {

    override val arrowType: ArrowType = I32
    override val byteWidth = Int.SIZE_BYTES

    companion object {
        private fun openValidityBuffer(al: BufferAllocator, valueCount: Int) =
            BitBuffer(al, valueCount)

        private fun openDataBuffer(al: BufferAllocator, valueCount: Int) =
            ExtensibleBuffer(al, valueCount.toLong() * Int.SIZE_BYTES)

        @JvmStatic
        @JvmOverloads
        fun open(al: BufferAllocator, name: String, nullable: Boolean, valueCount: Int = 0): IntVector =
            openValidityBuffer(al, valueCount).closeOnCatch { validityBuffer ->
                openDataBuffer(al, valueCount).closeOnCatch { dataBuffer ->
                    IntVector(name, nullable, valueCount, validityBuffer, dataBuffer)
                }
            }
    }

    override fun getInt(idx: Int) = getInt0(idx)
    override fun writeInt(v: Int) = writeInt0(v)
    override fun getLong(idx: Int) = getInt(idx).toLong()

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = getInt(idx)

    override fun writeObject0(value: Any) {
        if (value is Int) writeInt(value) else throw InvalidWriteObjectException(fieldType, value)
    }

    override fun writeValue0(v: ValueReader) = writeInt(v.readInt())

    override fun getMetaDouble(idx: Int) = getInt(idx).toDouble()

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getInt(idx).toDouble())

    override fun openSlice(al: BufferAllocator) =
        validityBuffer.openSlice(al).closeOnCatch { validityBuffer ->
            dataBuffer.openSlice(al).closeOnCatch { dataBuffer ->
                IntVector(name, nullable, valueCount, validityBuffer, dataBuffer)
            }
        }
}