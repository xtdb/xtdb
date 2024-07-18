package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Field as ArrowField

class Field(val name: String, val type: VectorType, val nullable: Boolean, val children: List<Field>) {
    fun newVector(al: BufferAllocator): Vector { return type.newVector(this, al) }

    internal val arrowField: ArrowField
        get() = ArrowField(name, FieldType(nullable, type.arrowType, null), children.map { it.arrowField })

    companion object {
        fun nullableI32(name: String) = Field(name, Int32Type, true, emptyList())
        fun nonNullableI32(name: String) = Field(name, Int32Type, false, emptyList())

        fun from(field: ArrowField): Field =
            Field(field.name, VectorType.from(field.type), field.fieldType.isNullable, field.children.map { from(it) })
    }
}