package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.util.CallBack

fun toListFieldType (fieldType: FieldType): FieldType {
    return if (fieldType.isNullable) {
        FieldType.nullable(ArrowType.List.INSTANCE)
    } else {
        FieldType.notNullable(ArrowType.List.INSTANCE)
    }
}

class SetVector(name: String, allocator: BufferAllocator, fieldType: FieldType, callBack: CallBack? = null) :
    XtExtensionVector<ListVector>(name, allocator, fieldType, ListVector(name, allocator, toListFieldType(fieldType), callBack)) {

    override fun getObject0(index: Int): Set<*> = HashSet(underlyingVector.getObject(index))

    override fun getField(): Field {
        val field = super.getField()
        return Field(field.name, field.fieldType, underlyingVector.field.children)
    }
}
