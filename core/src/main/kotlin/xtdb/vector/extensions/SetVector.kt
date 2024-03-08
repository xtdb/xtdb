package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.util.CallBack

class SetVector(name: String, allocator: BufferAllocator, fieldType: FieldType, callBack: CallBack? = null) :
    XtExtensionVector<ListVector>(name, allocator, fieldType, ListVector(name, allocator, fieldType, callBack)) {

    override fun getObject(index: Int): Set<*> = HashSet(underlyingVector.getObject(index))

    override fun getField(): Field {
        val field = super.getField()
        return Field(field.name, field.fieldType, underlyingVector.field.children)
    }
}
