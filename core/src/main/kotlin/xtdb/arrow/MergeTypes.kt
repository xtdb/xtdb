package xtdb.arrow

import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.VectorType.Companion.asType
import xtdb.arrow.VectorType.Companion.fixedSizeList
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.setTypeOf
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.unionOf
import xtdb.trie.ColumnName
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

    private fun mergeList(nullable: Boolean, elType: VectorType?) {
        if (nullable) nullableList = true
        listElType = (listElType ?: MergeTypes()).merge(elType)
    }

    private fun Pair<MergeTypes, Boolean>.merge(nullable: Boolean, type: VectorType?) =
        Pair(first.merge(type), second || nullable)

    private fun mergeFixedSizeList(size: Int, nullable: Boolean, elType: VectorType?) {
        fixedSizeLists = (fixedSizeLists ?: mutableMapOf()).apply {
            compute(size) { _, existing ->
                (existing ?: Pair(MergeTypes(), false)).merge(nullable, elType)
            }
        }
    }

    private fun mergeSet(nullable: Boolean, elType: VectorType?) {
        if (nullable) nullableSet = true
        setElType = (setElType ?: MergeTypes()).merge(elType)
    }

    private fun mergeTsTzRange(nullable: Boolean, elType: VectorType?) {
        if (nullable) nullableTsTzRange = true
        tstzRangeElType = (tstzRangeElType ?: MergeTypes()).merge(elType)
    }

    private fun mergeStruct(nullable: Boolean, children: Map<String, VectorType>) {
        if (nullable) nullableStruct = true

        val existingKeys = structs?.keys
        val default = { MergeTypes().also { if (existingKeys != null) it.nullable = true } }

        structs = (structs ?: mutableMapOf()).also { structs ->
            for ((name, type) in children) {
                structs.compute(name) { _, existing -> (existing ?: default()).merge(type) }
            }

            for (absent in existingKeys?.minus(children.keys).orEmpty()) {
                structs[absent]?.nullable = true
            }
        }
    }

    private fun mergeUnion(children: Map<String, VectorType>) {
        for ((_, type) in children)
            merge(type)
    }

    private fun Map<FieldName, VectorType>.firstValueOrNull() = entries.firstOrNull()?.value

    fun merge(arrowType: ArrowType, nullable: Boolean, children: Map<String, VectorType>): MergeTypes = apply {
        arrowType.accept(object : ArrowType.ArrowTypeVisitor<Unit> {
            override fun visit(arrowType: ArrowType.Null) {
                this@MergeTypes.nullable = true
            }

            override fun visit(arrowType: ArrowType.Struct) = mergeStruct(nullable, children)
            override fun visit(arrowType: ArrowType.List) = mergeList(nullable, children.firstValueOrNull())
            override fun visit(arrowType: ArrowType.LargeList) = unsupported("LargeList")
            override fun visit(arrowType: ArrowType.FixedSizeList) =
                mergeFixedSizeList(arrowType.listSize, nullable, children.firstValueOrNull())

            override fun visit(arrowType: ArrowType.Union) = mergeUnion(children)

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
                    SetType -> mergeSet(nullable, children.firstValueOrNull())
                    TsTzRangeType -> mergeTsTzRange(nullable, children.firstValueOrNull())

                    TransitType, UriType, UuidType, KeywordType,
                    IntervalMDMType,
                    RegClassType, RegProcType
                        -> mergeScalar(arrowType, nullable)

                    else -> unsupported("extension: $arrowType")
                }
        })
    }

    fun merge(field: Field?) = apply { field?.let { merge(it.type, it.isNullable, it.children.associate { c -> c.name to c.asType }) } }
    fun merge(type: VectorType?) = apply { type?.let { merge(it.arrowType, it.nullable, it.children) } }

    val asType: VectorType
        get() {
            val listType = listElType?.let { maybe(listTypeOf(it.asType), nullableList) }

            val fixedSizeListTypes = fixedSizeLists
                ?.map { (size, tm) -> fixedSizeList(size, maybe(tm.first.asType, tm.second)) }
                .orEmpty()

            val setType = setElType?.let { maybe(setTypeOf(it.asType), nullableSet) }

            val tstzRangeType = tstzRangeElType?.let {
                maybe(TsTzRangeType, nullableTsTzRange, LIST_ELS_NAME to it.asType)
            }

            val structType = structs?.let { maybe(structOf(it.mapValues { e -> e.value.asType }), nullableStruct) }

            val nullType = if (nullable) VectorType.NULL else null

            val types =
                scalars.map { maybe(VectorType(it.key), it.value) }
                    .plus(fixedSizeListTypes)
                    .plus(listOfNotNull(listType, setType, structType, tstzRangeType))

            return when (types.size) {
                0 -> VectorType.NULL
                1 -> types.first().let { if (nullable) maybe(it) else it }
                else -> unionOf((types + listOfNotNull(nullType)).associateBy { t -> t.arrowType.toLeg() })
            }
        }

    companion object {
        internal fun mergeTypes0(types: Collection<VectorType?>) =
            MergeTypes()
                .apply {
                    for (type in types)
                        merge(type)
                }
                .asType

        private val cache = Caffeine.newBuilder()
            .maximumSize(4096)
            .build<Set<VectorType>, VectorType> { mergeTypes0(it) }

        @JvmStatic
        fun mergeTypes(types: Collection<VectorType?>): VectorType = cache[types.mapNotNullTo(mutableSetOf()) { it }]

        fun mergeTypes(vararg types: VectorType?): VectorType = mergeTypes(types.toList())

        @JvmStatic
        fun mergeFields(fields: Collection<Field?>): VectorType = mergeTypes(fields.map { it?.asType })

        fun mergeFields(vararg fields: Field?): VectorType = mergeFields(fields.toList())
    }
}

