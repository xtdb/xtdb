package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FixedSizeBinaryVector
import org.apache.arrow.vector.types.pojo.FieldType
import java.nio.ByteBuffer
import java.util.*

class UuidVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<FixedSizeBinaryVector>(name, allocator, fieldType, FixedSizeBinaryVector(name, allocator, 16)) {

    init {
        require(fieldType.type == UuidType)
    }

    override fun getObject0(index: Int): UUID {
        val bb = ByteBuffer.wrap(underlyingVector.getObject(index))
        return UUID(bb.getLong(), bb.getLong())
    }
}
