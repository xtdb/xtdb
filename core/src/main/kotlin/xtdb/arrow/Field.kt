package xtdb.arrow

class Field(val name: String, val type: VectorType, val nullable: Boolean, val children: List<Field>) {
    companion object {
        fun nullableI32(name: String) = Field(name, Int32Type, true, emptyList())
        fun nonNullableI32(name: String) = Field(name, Int32Type, false, emptyList())
    }
}