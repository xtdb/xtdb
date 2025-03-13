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

class SetVector : XtExtensionVector<ListVector> {

    constructor( name: String, allocator: BufferAllocator, fieldType: FieldType, callBack: CallBack? = null) :
            super(name, allocator, fieldType, ListVector(name, allocator, toListFieldType(fieldType), callBack)) {
        require(fieldType.type is SetType)
    }

    constructor(field: Field, allocator: BufferAllocator, callBack: CallBack? = null) :
            super(field, allocator, ListVector(field.name, allocator, toListFieldType(field.fieldType), callBack)) {
        require(field.fieldType.type is SetType)
    }

    // the overriding of the XtExtensionVector.getTransferPair are only needed because createVector
    // on ListField throws when no child is provided, see #3377
    override fun getTransferPair(allocator: BufferAllocator) =
        makeTransferPair(SetVector(field.name, allocator, FieldType.nullable(SetType), null))

    override fun getTransferPair(field: Field, allocator: BufferAllocator) =
        makeTransferPair(SetVector(field, allocator, null))

    override fun getObject0(index: Int): Set<*> = HashSet(underlyingVector.getObject(index))

    override fun getField(): Field {
        val field = super.getField()
        return Field(field.name, field.fieldType, underlyingVector.field.children)
    }
}
