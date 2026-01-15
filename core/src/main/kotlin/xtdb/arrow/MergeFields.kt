package xtdb.arrow

import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.vector.extensions.*

data class MergeFields(
    private var nullable: Boolean = false,
    private val scalars: MutableMap<ArrowType, Boolean> = mutableMapOf(),
    private var listyTypes: MutableMap<ArrowType, Pair<MergeFields, Boolean>> = mutableMapOf(),
    private var nullableStruct: Boolean = false,
    private var structs: MutableMap<FieldName, MergeFields>? = null,
) {

    private fun mergeScalar(arrowType: ArrowType, nullable: Boolean) {
        scalars.compute(arrowType) { _, existing -> (existing ?: false) || nullable }
    }

    private fun mergeListy(listField: Field) {
        listyTypes.compute(listField.type) { _, existing ->
            val (mergeFields, nullable) = (existing ?: Pair(MergeFields(), false))
            Pair(
                listField.children.firstOrNull()?.let { mergeFields.merge(it) } ?: mergeFields,
                nullable || listField.isNullable
            )
        }
    }

    private fun mergeStruct(nullable: Boolean, children: List<Field>) {
        if (nullable) nullableStruct = true

        val existingKeys = structs?.keys
        val default = { MergeFields().also { if (existingKeys != null) it.nullable = true } }

        structs = (structs ?: mutableMapOf()).also { structs ->
            for (child in children) {
                structs.compute(child.name) { _, existing -> (existing ?: default()).merge(child) }
            }

            for (absent in existingKeys?.minus(children.mapTo(mutableSetOf()) { it.name }).orEmpty()) {
                structs[absent]?.nullable = true
            }
        }
    }

    private fun mergeUnion(children: List<Field>) {
        for (child in children)
            merge(child)
    }

    fun merge(field: Field): MergeFields = apply {
        val nullable = field.isNullable

        field.type.accept(object : ArrowType.ArrowTypeVisitor<Unit> {
            override fun visit(arrowType: ArrowType.Null) {
                this@MergeFields.nullable = true
            }

            override fun visit(arrowType: ArrowType.Struct) = mergeStruct(nullable, field.children)
            override fun visit(arrowType: ArrowType.List) = mergeListy(field)
            override fun visit(arrowType: ArrowType.LargeList) = unsupported("LargeList")
            override fun visit(arrowType: ArrowType.FixedSizeList) = mergeListy(field)

            override fun visit(arrowType: ArrowType.Union) = mergeUnion(field.children)

            override fun visit(arrowType: ArrowType.Map) = TODO("Not yet implemented")
            override fun visit(arrowType: ArrowType.Int) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.FloatingPoint) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Utf8) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Utf8View) = unsupported("Utf8View")
            override fun visit(arrowType: ArrowType.LargeUtf8) = unsupported("LargeUtf8")
            override fun visit(arrowType: ArrowType.Binary) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.BinaryView) = unsupported("BinaryView")
            override fun visit(arrowType: ArrowType.LargeBinary) = unsupported("LargeBinary")
            override fun visit(arrowType: ArrowType.FixedSizeBinary) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Bool) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Decimal) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Date) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Time) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Timestamp) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Interval) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.Duration) = mergeScalar(arrowType, nullable)
            override fun visit(arrowType: ArrowType.ListView) = unsupported("ListView")
            override fun visit(arrowType: ArrowType.LargeListView) = unsupported("LargeListView")
            override fun visit(arrowType: ArrowType.RunEndEncoded) = unsupported("RunEndEncoded")

            override fun visit(arrowType: ArrowType.ExtensionType) =
                when (arrowType) {
                    SetType, TsTzRangeType -> mergeListy(field)

                    TransitType, UriType, UuidType, KeywordType,
                    IntervalMDMType,
                    OidType, RegClassType, RegProcType
                        -> mergeScalar(arrowType, nullable)

                    else -> unsupported("extension: $arrowType")
                }
        })
    }

    val asLegField: Field get() = toField(null)

    fun toField(fieldName: FieldName?): Field {
        val listyFields = listyTypes
            .map { (arrowType, tm) ->
                Field(arrowType.toLeg(), FieldType(tm.second, arrowType, null), listOf(tm.first.toField(LIST_ELS_NAME)))
            }

        val structField = structs?.let {
            Field(
                STRUCT_TYPE.toLeg(),
                FieldType(nullableStruct, STRUCT_TYPE, null),
                it.map { (key, type) -> type.toField(key) }
            )
        }

        val fields =
            scalars
                .map { (arrowType, nullable) ->
                    Field(arrowType.toLeg(), FieldType(nullable, arrowType, null), emptyList())
                }
                .plus(listyFields)
                .plus(listOfNotNull(structField))

        return when (fields.size) {
            0 -> Field(fieldName ?: NULL_TYPE.toLeg(), FieldType.nullable(NULL_TYPE), emptyList())

            1 -> fields.single().let {
                Field(
                    fieldName ?: it.type.toLeg(),
                    if (nullable && !it.isNullable) FieldType.nullable(it.type) else it.fieldType,
                    it.children
                )
            }

            else ->
                Field(
                    fieldName ?: UNION_TYPE.toLeg(), FieldType.notNullable(UNION_TYPE),
                    fields + listOfNotNull(if (nullable) nullField else null)
                )
        }
    }

    companion object {
        private val nullField = Field(NULL_TYPE.toLeg(), FieldType.nullable(NULL_TYPE), emptyList())

        internal fun mergeFields0(fields: Set<Field>) =
            MergeFields().apply { fields.forEach { merge(it) } }.asLegField

        private val cache = Caffeine.newBuilder()
            .maximumSize(4096)
            .build<Set<Field>, Field> { mergeFields0(it) }

        @JvmStatic
        fun mergeFields(fields: Iterable<Field?>): Field = cache[fields.filterNotNullTo(mutableSetOf())]
    }
}

