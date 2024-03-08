package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry
import org.apache.arrow.vector.types.pojo.FieldType

object TransitType : XtExtensionType("xt/transit+msgpack", Binary.INSTANCE) {
    init {
        ExtensionTypeRegistry.register(this)
    }

    override fun deserialize(serializedData: String) = this

    override fun getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator): FieldVector =
        TransitVector(name, allocator, fieldType)
}
