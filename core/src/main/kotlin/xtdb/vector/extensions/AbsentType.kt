package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry
import org.apache.arrow.vector.types.pojo.FieldType

object AbsentType : XtExtensionType("absent", Null.INSTANCE) {
    init {
        ExtensionTypeRegistry.register(this)
    }

    override fun deserialize(serializedData: String) = this

    override fun getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator) =
        AbsentVector(name, allocator, fieldType)
}
