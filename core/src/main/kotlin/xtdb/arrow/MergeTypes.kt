package xtdb.arrow

import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.error.Fault
import xtdb.arrow.VectorType.*
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.fromLegs

data class MergeTypes(
    private val scalars: MutableSet<Scalar> = mutableSetOf(),
    private val listyTypes: MutableMap<ArrowType, MergeTypes> = mutableMapOf(),
    private var structKeys: MutableMap<FieldName, MergeTypes>? = null,
    private var nullable: Boolean = false,
) {

    private fun merge(type: VectorType) {
        when (type) {
            Null -> nullable = true

            Nothing -> {} // bottom: contributes nothing to the merge

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

        /**
         * [mergeTypes] with `Nothing` restored as the identity.
         *
         * `⊔` is not idempotent at the bottom today - `mergeTypes(Nothing, Nothing)` returns `Null` (#5871) -
         * so a join over sources that all have nothing to say reports a dataless column as nullable.
         *
         * Delete this and call [mergeTypes] directly once `fromLegs(∅)` returns the bottom.
         */
        @JvmStatic
        fun joinContributions(types: Collection<VectorType>): VectorType =
            if (types.all { it == Nothing }) Nothing else mergeTypes(types)

        /**
         * A source with nothing to say contributes [VectorType.Nothing], the lattice bottom - never a Kotlin
         * null. A null is rejected rather than discarded, because discarding one drops a contribution from
         * the join without trace: that is how #5855 stayed hidden, an accessor returning null for "no such
         * column" looking like a legitimate argument.
         *
         * The signature holds Kotlin callers to that. It cannot hold Clojure ones - erasure means there is no
         * per-element check on a generic collection - hence the explicit rejection below.
         */
        @JvmStatic
        fun mergeTypes(types: Collection<VectorType>): VectorType {
            val set = mutableSetOf<VectorType>()

            for (type in types) {
                // The compiler calls this comparison senseless because the element type is non-null; it is
                // reachable anyway, from Clojure through erasure. Do not delete it. Without it a nil falls
                // through `merge`'s exhaustive `when` as a bare NoWhenBranchMatchedException - no anomaly
                // category, no indication of which argument was bad. Checking here rather than inside the
                // cache loader also keeps the offending caller's frame directly above the throw.
                @Suppress("SENSELESS_COMPARISON")
                if (type == null) throw Fault("null passed to mergeTypes - a source with nothing to say contributes Nothing", "xtdb/merge-types-null")

                set.add(type)
            }

            return cache[set]
        }

        fun mergeTypes(vararg types: VectorType): VectorType = mergeTypes(types.toList())
    }
}

