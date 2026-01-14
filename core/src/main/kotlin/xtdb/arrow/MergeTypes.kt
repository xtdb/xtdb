package xtdb.arrow

import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.VectorType.*
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.fromLegs
import xtdb.trie.ColumnName

data class MergeTypes(
    private val scalars: MutableSet<Scalar> = mutableSetOf(),
    private val listyTypes: MutableMap<ArrowType, MergeTypes> = mutableMapOf(),
    private var structKeys: MutableMap<ColumnName, MergeTypes>? = null,
    private var nullable: Boolean = false,
) {

    private fun merge(type: VectorType) {
        when (type) {
            Null -> nullable = true

            is Poly -> type.legs.forEach { merge(it) }

            is Maybe -> {
                merge(Null); merge(type.mono)
            }

            is Listy ->
                listyTypes.compute(type.arrowType) { _, existing ->
                    (existing ?: MergeTypes()).also { it.merge(type.elType) }
                }

            is Scalar -> scalars.add(type)

            is Struct -> {
                val existingKeys = structKeys?.keys
                val default = { MergeTypes().also { if (existingKeys != null) it.nullable = true } }

                structKeys = (structKeys ?: mutableMapOf()).also { structKeys ->
                    for ((name, type) in type.children) {
                        structKeys.compute(name) { _, existing -> (existing ?: default()).also { it.merge(type) } }
                    }

                    for (absent in existingKeys?.minus(type.children.keys).orEmpty()) {
                        structKeys[absent]?.nullable = true
                    }
                }
            }
        }
    }

    val asType: VectorType
        get() {
            val nullType = if (nullable) Null else null
            val listyTypes = listyTypes .map { (arrowType, el) -> Listy(arrowType, el.asType) }
            val structType = structKeys?.let { structOf(it.mapValues { e -> e.value.asType }) }

            return fromLegs(scalars + listyTypes + listOfNotNull(structType, nullType))
        }

    companion object {
        internal fun mergeTypes0(types: Iterable<VectorType>) =
            MergeTypes().apply { types.forEach(::merge) }.asType

        private val cache = Caffeine.newBuilder()
            .maximumSize(4096)
            .build<Set<VectorType>, VectorType> { mergeTypes0(it) }

        @JvmStatic
        fun mergeTypes(types: Collection<VectorType?>): VectorType = cache[types.mapNotNullTo(mutableSetOf()) { it }]

        fun mergeTypes(vararg types: VectorType?): VectorType = mergeTypes(types.toList())
    }
}

