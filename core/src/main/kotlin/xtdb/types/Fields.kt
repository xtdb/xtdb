package xtdb.types

import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.types.Fields.asFieldList
import xtdb.types.NamelessField.Companion.asField
import xtdb.types.NamelessField.Companion.asNullableField
import org.apache.arrow.vector.types.pojo.ArrowType.List.INSTANCE as LIST_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.Null.INSTANCE as NULL_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE
import org.apache.arrow.vector.types.pojo.Field as ArrowField

typealias FieldName = String

fun Schema(vararg fields: Pair<FieldName, NamelessField>) = Schema(fields.asIterable().asFieldList)

data class NamelessField(val fieldType: FieldType, val children: List<ArrowField> = emptyList()) {
    constructor(fieldType: FieldType, vararg children: Pair<FieldName, NamelessField>)
            : this(fieldType, children.asIterable().asFieldList)

    fun toArrowField(name: FieldName) = ArrowField(name, fieldType, children)

    val arrowType: ArrowType get() = fieldType.type

    val nullable get() = copy(fieldType = FieldType.nullable(fieldType.type))

    companion object {
        fun ArrowType.asField(vararg children: Pair<FieldName, NamelessField>) =
            NamelessField(FieldType.notNullable(this), *children)

        val ArrowType.asField get() = asField()

        fun ArrowType.asNullableField(vararg children: Pair<FieldName, NamelessField>) =
            NamelessField(FieldType.nullable(this), *children)

        val ArrowType.asNullableField get() = asNullableField()

        val MinorType.asField get() = type.asField

        fun nullable(field: NamelessField) = field.nullable
    }
}

val ArrowField.asPair get() = name to NamelessField(fieldType, children)

@Suppress("FunctionName")
object Fields {

    val NULL = NULL_TYPE.asNullableField
    val BOOL = MinorType.BIT.asField
    val I32 = MinorType.INT.asField
    val I64 = MinorType.BIGINT.asField
    val UTF8 = MinorType.VARCHAR.asField
    val VAR_BINARY = MinorType.VARBINARY.asField

    val TEMPORAL = ArrowType.Timestamp(MICROSECOND, "UTC").asField
    val IID = ArrowType.FixedSizeBinary(16).asField

    fun Union(vararg legs: Pair<FieldName, NamelessField>) =
        ArrowType.Union(UnionMode.Dense, null).asField(*legs)

    fun Struct(vararg fields: Pair<FieldName, NamelessField>) =
        STRUCT_TYPE.asField(*fields)

    fun List(el: NamelessField, elName: FieldName = "\$data\$") =
        LIST_TYPE.asField(elName to el)

    fun Map(
        key: Pair<FieldName, NamelessField>, value: Pair<FieldName, NamelessField>,
        sorted: Boolean = true, entriesName: FieldName = "\$entries\$",
    ) =
        ArrowType.Map(sorted)
            .asField(entriesName to Struct(key, value))

    internal val Iterable<Pair<FieldName, NamelessField>>.asFieldList
        get() = map { it.second.toArrowField(it.first) }
}


