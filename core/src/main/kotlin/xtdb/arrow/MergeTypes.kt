package xtdb.arrow

import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.*
import org.apache.arrow.vector.types.pojo.ArrowType.List
import xtdb.arrow.VectorType.Companion.fixedSizeList
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.setTypeOf
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.unionOf
import xtdb.trie.ColumnName
import xtdb.vector.extensions.*

data class MergeTypes(
    private var nullable: Boolean = false,
    private val scalars: MutableMap<ArrowType, Boolean> = mutableMapOf(),
    private var listyTypes: MutableMap<ArrowType, Pair<MergeTypes, Boolean>> = mutableMapOf(),
    private var nullableStruct: Boolean = false,
    private var structs: MutableMap<ColumnName, MergeTypes>? = null,
) {

    private fun mergeScalar(arrowType: ArrowType, nullable: Boolean) {
        scalars.compute(arrowType) { _, existing -> (existing ?: false) || nullable }
    }

    private fun mergeListy(type: VectorType) {
        listyTypes.compute(type.arrowType) { _, existing ->
            val (mergeTypes, nullable) = (existing ?: Pair(MergeTypes(), false))
            Pair(
                type.children.firstValueOrNull()?.let { mergeTypes.merge(it) } ?: mergeTypes,
                nullable || type.nullable
            )
        }
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

    fun merge(type: VectorType?) = apply {
        type?.let {
            val nullable = it.nullable

            it.arrowType.accept(object : ArrowTypeVisitor<Unit> {
                override fun visit(arrowType: Null) {
                    this@MergeTypes.nullable = true
                }

                override fun visit(arrowType: Struct) = mergeStruct(nullable, it.children)
                override fun visit(arrowType: List) = mergeListy(it)
                override fun visit(arrowType: LargeList) = unsupported("LargeList")
                override fun visit(arrowType: FixedSizeList) = mergeListy(it)

                override fun visit(arrowType: Union) = mergeUnion(it.children)

                override fun visit(arrowType: ArrowType.Map) = TODO("Not yet implemented")
                override fun visit(arrowType: ArrowType.Int) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: FloatingPoint) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Utf8) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Utf8View) = unsupported("Utf8View")
                override fun visit(arrowType: LargeUtf8) = unsupported("LargeUtf8")
                override fun visit(arrowType: Binary) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: BinaryView) = unsupported("BinaryView")
                override fun visit(arrowType: LargeBinary) = unsupported("LargeBinary")
                override fun visit(arrowType: FixedSizeBinary) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Bool) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Decimal) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Date) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Time) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Timestamp) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Interval) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: Duration) = mergeScalar(arrowType, nullable)
                override fun visit(arrowType: ListView) = unsupported("ListView")
                override fun visit(arrowType: LargeListView) = unsupported("LargeListView")
                override fun visit(arrowType: RunEndEncoded) = unsupported("RunEndEncoded")

                override fun visit(arrowType: ExtensionType) =
                    when (it.arrowType) {
                        SetType, TsTzRangeType -> mergeListy(it)

                        TransitType, UriType, UuidType, KeywordType,
                        IntervalMDMType,
                        OidType, RegClassType, RegProcType
                            -> mergeScalar(arrowType, nullable)

                        else -> unsupported("extension: ${it.arrowType}")
                    }
            })
        }
    }

    val asType: VectorType
        get() {
            val listyTypesList = listyTypes
                .map { (arrowType, tm) ->
                    when (arrowType) {
                        is List -> maybe(listTypeOf(tm.first.asType), tm.second)
                        is FixedSizeList -> maybe(fixedSizeList(arrowType.listSize, tm.first.asType), tm.second)
                        SetType -> maybe(setTypeOf(tm.first.asType), tm.second)
                        TsTzRangeType -> maybe(TsTzRangeType, tm.second, LIST_ELS_NAME to tm.first.asType)
                        else -> throw IllegalStateException("Unexpected listy type: $arrowType")
                    }
                }

            val structType = structs?.let { maybe(structOf(it.mapValues { e -> e.value.asType }), nullableStruct) }

            val nullType = if (nullable) VectorType.NULL else null

            val types =
                scalars.map { maybe(VectorType(it.key), it.value) }
                    .plus(listyTypesList)
                    .plus(listOfNotNull(structType))

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
    }
}

