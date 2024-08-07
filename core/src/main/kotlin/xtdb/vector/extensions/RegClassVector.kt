package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.types.RegClass

class RegClassVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<IntVector>(name, allocator, fieldType, IntVector(name, allocator)) {

    override fun getObject0(index: Int): RegClass = RegClass(underlyingVector.get(index));
}
