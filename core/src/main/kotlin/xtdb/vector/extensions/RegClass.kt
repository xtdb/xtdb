package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.types.RegClass

class RegClassVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<IntVector>(name, allocator, fieldType, IntVector(name, allocator)) {

    init {
        require(fieldType.type == RegClassType)
    }

    override fun getObject0(index: Int): RegClass = RegClass(underlyingVector.get(index))
}

object RegClassType: XtExtensionType("xt/regclass", Types.MinorType.INT.getType()) {
    init {
        ExtensionTypeRegistry.register(this)
    }

    override fun deserialize(serializedData: String): ArrowType = this

    override fun getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator): FieldVector =
        RegClassVector(name, allocator, fieldType)
}