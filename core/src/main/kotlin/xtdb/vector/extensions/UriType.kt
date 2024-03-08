package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry
import org.apache.arrow.vector.types.pojo.FieldType

object UriType : XtExtensionType("uri", Utf8.INSTANCE) {
    init {
        ExtensionTypeRegistry.register(this)
    }

    override fun deserialize(serializedData: String) = this

    override fun getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator) =
        UriVector(name, allocator, fieldType)
}
