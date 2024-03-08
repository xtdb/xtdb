package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

class AbsentVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<NullVector>(name, allocator, fieldType, NullVector(name)) {

    override fun getObject(index: Int) = null
}
