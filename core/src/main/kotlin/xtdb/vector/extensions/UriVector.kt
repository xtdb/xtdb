package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.types.pojo.FieldType
import java.net.URI
import java.nio.charset.StandardCharsets

class UriVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<VarCharVector>(name, allocator, fieldType, VarCharVector(name, allocator)) {

    init {
        require(fieldType.type == UriType)
    }

    override fun getObject0(index: Int): URI = URI.create(String(underlyingVector[index], StandardCharsets.UTF_8))
}
