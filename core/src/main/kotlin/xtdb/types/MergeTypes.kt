package xtdb.types

import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.unsupported
import xtdb.trie.ColumnName
import xtdb.types.Type.Companion.asType
import xtdb.types.Type.Companion.listTypeOf
import xtdb.types.Type.Companion.maybe
import xtdb.types.Type.Companion.ofType
import xtdb.types.Type.Companion.structOf
import xtdb.vector.extensions.*

data class MergeTypes(
    private var nullableList: Boolean = false,
    private var listElType: MergeTypes? = null,
    private var fixedSizeLists: MutableMap<Int, Pair<MergeTypes, Boolean>>? = null,
    private var nullableSet: Boolean = false,
    private var setElType: MergeTypes? = null,
    private var nullableTsTzRange: Boolean = false,
    private var tstzRangeElType: MergeTypes? = null,
    private var nullableStruct: Boolean = false,
    private var structs: MutableMap<ColumnName, MergeTypes>? = null,
    private val scalars: MutableMap<ArrowType, Boolean> = mutableMapOf(),
    private var nullable: Boolean = false,
) {

    private fun mergeScalar(arrowType: ArrowType, nullable: Boolean) {
        scalars.compute(arrowType) { _, existing -> (existing ?: false) || nullable }
    }

    private fun mergeList(nullable: Boolean, elType: Field?) {
        if (nullable) nullableList = true
        listElType = (listElType ?: MergeTypes()).merge(elType)
    }

    private fun Pair<MergeTypes, Boolean>.merge(nullable: Boolean, type: Field?) =
        Pair(first.merge(type), second || nullable)

    private fun mergeFixedSizeList(size: Int, nullable: Boolean, elType: Field?) {
        fixedSizeLists = (fixedSizeLists ?: mutableMapOf()).apply {
            compute(size) { _, existing ->
                (existing ?: Pair(MergeTypes(), false)).merge(nullable, elType)
            }
        }
    }

    private fun mergeSet(nullable: Boolean, elType: Field?) {
        if (nullable) nullableSet = true
        setElType = (setElType ?: MergeTypes()).merge(elType)
    }

    private fun mergeTsTzRange(nullable: Boolean, elType: Field?) {
        if (nullable) nullableTsTzRange = true
        tstzRangeElType = (tstzRangeElType ?: MergeTypes()).merge(elType)
    }

    private fun mergeStruct(nullable: Boolean, children: Collection<Field>) {
        if (nullable) nullableStruct = true

        val existingKeys = structs?.keys
        val default = { MergeTypes().also { if (existingKeys != null) it.nullable = true } }

        structs = (structs ?: mutableMapOf()).also { structs ->
            for (child in children) {
                structs.compute(child.name) { _, existing -> (existing ?: default()).merge(child) }
            }

            for (absent in existingKeys?.minus(children.map { it.name }.toSet()).orEmpty()) {
                structs[absent]?.nullable = true
            }
        }
    }

    private fun mergeUnion(children: Collection<Field>) {
        for (child in children)
            merge(child.type, child.isNullable, child.children)
    }

    fun merge(arrowType: ArrowType, nullable: Boolean, children: Collection<Field>?): MergeTypes = apply {
        arrowType.accept(object : ArrowType.ArrowTypeVisitor<Unit> {
            override fun visit(arrowType: ArrowType.Null) {
                this@MergeTypes.nullable = true
            }

            override fun visit(arrowType: ArrowType.Struct) = mergeStruct(nullable, children.orEmpty())
            override fun visit(arrowType: ArrowType.List) = mergeList(nullable, children?.firstOrNull())
            override fun visit(arrowType: ArrowType.LargeList) = unsupported("LargeList")
            override fun visit(arrowType: ArrowType.FixedSizeList) =
                mergeFixedSizeList(arrowType.listSize, nullable, children?.firstOrNull())

            override fun visit(arrowType: ArrowType.Union) = mergeUnion(children.orEmpty())

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
                    SetType -> mergeSet(nullable, children?.firstOrNull())
                    TsTzRangeType -> mergeTsTzRange(nullable, children?.firstOrNull())

                    TransitType, UriType, UuidType, KeywordType,
                    IntervalMDMType,
                    RegClassType, RegProcType
                        -> mergeScalar(arrowType, nullable)

                    else -> unsupported("extension: $arrowType")
                }
        })
    }

    fun merge(field: Field?) = apply { field?.let { merge(it.type, it.isNullable, it.children) } }
    fun merge(type: Type?) = apply { type?.let { merge(it.arrowType, it.nullable, it.children) } }

    val asType: Type
        get() {
            val listType = listElType?.let { maybe(listTypeOf(it.asType), nullableList) }

            val fixedSizeListTypes = fixedSizeLists
                ?.map { (size, tm) ->
                    Type.fixedSizeList(size, maybe(tm.first.asType, tm.second))
                }
                .orEmpty()

            val setType = setElType?.let { Type.setTypeOf(it.asType) }?.let { maybe(it, nullableSet) }
            val tstzRangeType = tstzRangeElType?.let {
                maybe(TsTzRangeType, nullableTsTzRange, listOf(LIST_ELS_NAME ofType it.asType))
            }

            val structType =
                structs?.let { structs ->
                    maybe(structOf(structs.map { it.key ofType it.value.asType }), nullableStruct)
                }

            val nullType = if (nullable) Type.NULL else null

            val types =
                scalars.map { maybe(Type(it.key), it.value) }
                    .plus(fixedSizeListTypes)
                    .plus(listOfNotNull(listType, setType, structType, tstzRangeType))

            return when (types.size) {
                0 -> Type.NULL
                1 -> types.first().let { if (nullable) maybe(it) else it }
                else -> Type.unionOf((types + listOfNotNull(nullType)).map { t -> t.asLegField })
            }
        }

    companion object {
        internal fun mergeTypes0(types: Collection<Type?>) =
            MergeTypes()
                .apply {
                    for (type in types)
                        merge(type)
                }
                .asType

        private val cache = Caffeine.newBuilder()
            .maximumSize(4096)
            .build<Set<Type>, Type> { mergeTypes0(it) }

        @JvmStatic
        fun mergeTypes(types: Collection<Type?>): Type = cache[types.mapNotNullTo(mutableSetOf()) { it }]

        fun mergeTypes(vararg types: Type?): Type = mergeTypes(types.toList())

        @JvmStatic
        fun mergeFields(fields: Collection<Field?>): Type = mergeTypes(fields.map { it?.asType })

        fun mergeFields(vararg fields: Field?): Type = mergeFields(fields.toList())
    }
}

