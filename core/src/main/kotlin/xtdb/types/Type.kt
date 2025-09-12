@file:JvmName("Types")

package xtdb.types

import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType.List.INSTANCE as LIST_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.Null.INSTANCE as NULL_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE

typealias FieldName = String

private val Iterable<Pair<FieldName, Type>>.asFieldList
    get() = map { it.second.toField(it.first) }

fun Schema(vararg fields: Pair<FieldName, Type>) = Schema(fields.asIterable().asFieldList)

data class Type(val arrowType: ArrowType, val nullable: Boolean = false, val children: List<Field> = emptyList()) {
    constructor(
        arrowType: ArrowType, nullable: Boolean = false, vararg children: Pair<FieldName, Type>
    ) : this(arrowType, nullable, children.asIterable().asFieldList)

    constructor(
        arrowType: ArrowType, vararg children: Pair<FieldName, Type>
    ) : this(arrowType, false, children = children)

    val fieldType get() = FieldType(nullable, arrowType, null)

    fun toField(name: FieldName) = Field(name, fieldType, children)

    fun nullable() = copy(nullable = true)

    companion object {

        @JvmField
        val NULL = Type(NULL_TYPE).nullable()

        @JvmField
        val BOOL = Type(MinorType.BIT.type)

        @JvmField
        val I32 = Type(MinorType.INT.type)

        @JvmField
        val I64 = Type(MinorType.BIGINT.type)

        @JvmField
        val UTF8 = Type(MinorType.VARCHAR.type)

        @JvmField
        val TEMPORAL = Type(ArrowType.Timestamp(MICROSECOND, "UTC"), children = arrayOf())

        @JvmField
        val IID = Type(ArrowType.FixedSizeBinary(16), children = arrayOf())

        fun union(vararg legs: Pair<FieldName, Type>) =
            Type(ArrowType.Union(UnionMode.Dense, null), children = legs)

        fun struct(vararg fields: Pair<FieldName, Type>) =
            Type(STRUCT_TYPE, children = fields)

        fun struct(fields: List<Field>) = Type(STRUCT_TYPE, children = fields)

        fun list(el: Type, elName: FieldName = $$"$data$") =
            Type(LIST_TYPE, elName to el)

        fun map(
            key: Pair<FieldName, Type>, value: Pair<FieldName, Type>,
            sorted: Boolean = true, entriesName: FieldName = $$"$entries$",
        ) =
            Type(ArrowType.Map(sorted), children = arrayOf(entriesName to struct(key, value)))
    }
}