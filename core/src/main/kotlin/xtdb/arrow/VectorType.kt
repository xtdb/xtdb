@file:JvmName("Types")

package xtdb.arrow

import org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE
import org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.vector.extensions.SetType

typealias FieldName = String

fun schema(vararg fields: Field) = Schema(fields.asIterable())

internal const val LIST_ELS_NAME = $$"$data$"

data class VectorType(val arrowType: ArrowType, val nullable: Boolean = false, val children: List<Field> = emptyList()) {

    val fieldType get() = FieldType(nullable, arrowType, null)

    val asLegField get() = Field(arrowType.toLeg(), fieldType, children)

    companion object {

        fun maybe(type: VectorType, nullable: Boolean = true) = type.copy(nullable = nullable)
        fun maybe(type: ArrowType, nullable: Boolean = true, children: List<Field> = emptyList()) =
            VectorType(type, nullable, children)

        fun maybe(type: ArrowType, vararg children: Field) = maybe(type, children = children.toList())
        fun just(type: ArrowType, children: List<Field> = emptyList()) = VectorType(type, false, children)
        fun just(type: ArrowType, vararg children: Field) = just(type, children.toList())

        @JvmField
        val NULL = VectorType(MinorType.NULL.type, true)

        @JvmField
        val BOOL = VectorType(MinorType.BIT.type)

        @JvmField
        val I32 = VectorType(MinorType.INT.type)

        @JvmField
        val I64 = VectorType(MinorType.BIGINT.type)

        @JvmField
        val F32 = VectorType(ArrowType.FloatingPoint(SINGLE))

        @JvmField
        val F64 = VectorType(ArrowType.FloatingPoint(DOUBLE))

        @JvmField
        val UTF8 = VectorType(MinorType.VARCHAR.type)

        @JvmField
        val TEMPORAL = VectorType(ArrowType.Timestamp(MICROSECOND, "UTC"))

        @JvmField
        val IID = VectorType(ArrowType.FixedSizeBinary(16))

        @JvmField
        val LIST_TYPE: ArrowType = MinorType.LIST.type

        @JvmField
        val STRUCT_TYPE: ArrowType = MinorType.STRUCT.type

        @JvmField
        val UNION_TYPE: ArrowType = MinorType.DENSEUNION.type

        fun unionOf(vararg legs: Field) = unionOf(legs.toList())
        fun unionOf(legs: List<Field>) = VectorType(ArrowType.Union(UnionMode.Dense, null), children = legs)
        fun FieldName.asUnionOf(vararg legs: Field) = asUnionOf(legs.toList())
        infix fun FieldName.asUnionOf(legs: List<Field>) = ofType(unionOf(legs))

        fun structOf(vararg fields: Field) = structOf(fields.toList())
        fun structOf(fields: List<Field>) = VectorType(STRUCT_TYPE, children = fields)
        fun FieldName.asStructOf(vararg fields: Field) = asStructOf(fields.toList())
        infix fun FieldName.asStructOf(fields: List<Field>) = ofType(structOf(fields))

        fun listTypeOf(el: VectorType, elName: FieldName = LIST_ELS_NAME) =
            just(LIST_TYPE, elName ofType el)

        infix fun FieldName.asListOf(el: VectorType) = this ofType listTypeOf(el)

        fun fixedSizeList(size: Int, el: VectorType, elName: FieldName = LIST_ELS_NAME) =
            just(ArrowType.FixedSizeList(size), elName ofType el)

        fun setTypeOf(el: VectorType, nullable: Boolean = false, elName: FieldName = LIST_ELS_NAME) =
            maybe(SetType, nullable, listOf(elName ofType el))

        fun mapTypeOf(
            key: Field, value: Field,
            sorted: Boolean = true, entriesName: FieldName = $$"$entries$",
        ) =
            just(ArrowType.Map(sorted), entriesName.asStructOf(key, value))

        @JvmStatic
        infix fun FieldName.ofType(type: VectorType) = Field(this, type.fieldType, type.children)

        val Field.asType get() = VectorType(type, isNullable, children)
    }
}