package xtdb.arrow

import clojure.lang.*
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.safeMap
import xtdb.vector.ValueVectorReader
import java.util.*

interface RelationReader : ILookup, Seqable, Counted, AutoCloseable {
    val schema: Schema get() = Schema(vectors.map { it.field })
    val rowCount: Int

    val vectors: Collection<VectorReader>

    fun vectorForOrNull(name: String): VectorReader?
    fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    operator fun get(name: String) = vectorFor(name)

    operator fun get(idx: Int, keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD): Map<*, Any?> =
        vectors.associate { keyFn.denormalize(it.name) to it.getObject(idx, keyFn) }

    fun openSlice(al: BufferAllocator): RelationReader =
        vectors
            .safeMap { it.openSlice(al) }
            .closeAllOnCatch { slicedVecs -> from(slicedVecs, rowCount) }

    fun openDirectSlice(al: BufferAllocator) =
        vectors
            .safeMap { it.openDirectSlice(al) }
            .closeAllOnCatch { vectors -> Relation(al, vectors, rowCount) }

    fun select(idxs: IntArray): RelationReader = from(vectors.map { it.select(idxs) }, idxs.size)
    fun select(startIdx: Int, len: Int): RelationReader = from(vectors.map { it.select(startIdx, len) }, len)

    override fun close() = vectors.closeAll()

    fun toTuples() = toTuples(KEBAB_CASE_KEYWORD)

    fun toTuples(keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD) =
        List(rowCount) { idx -> vectors.map { it.getObject(idx, keyFn) } }

    fun toMaps() = toMaps(KEBAB_CASE_KEYWORD)

    fun toMaps(keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD) =
        List(rowCount) { idx ->
            PersistentHashMap.create(
                vectors
                    .associate {
                        Pair(
                            keyFn.denormalize(it.name),
                            it.getObject(idx, keyFn)
                        )
                    }
                    .filterValues { it != null }
            ) as Map<*, *>
        }

    private class FromCols(
        private val cols: SequencedMap<String, VectorReader>, override val rowCount: Int
    ) : RelationReader {
        override fun vectorForOrNull(name: String) = cols[name]
        override val vectors get() = cols.values
    }

    companion object {
        fun from(cols: List<VectorReader>): RelationReader =
            FromCols(cols.associateByTo(linkedMapOf()) { it.name }, cols.firstOrNull()?.valueCount ?: 0)

        @JvmStatic
        fun from(cols: Iterable<VectorReader>, rowCount: Int): RelationReader =
            FromCols(cols.associateByTo(linkedMapOf()) { it.name }, rowCount)

        @JvmStatic
        fun from(root: VectorSchemaRoot): RelationReader =
            from(
                root.fieldVectors.map { v -> ValueVectorReader.from(v) },
                root.rowCount
            )

        @JvmStatic
        fun concatCols(rel1: RelationReader, rel2: RelationReader): RelationReader {
            if (rel1.vectors.isEmpty()) return rel2
            if (rel2.vectors.isEmpty()) return rel1
            assert(rel1.rowCount == rel2.rowCount) { "Cannot concatenate relations with different row counts" }

            return from(rel1.vectors + rel2.vectors, rel1.rowCount)
        }

        @Suppress("unused")
        @JvmField
        // naming from Oracle - zero cols, one row
        val DUAL = from(emptyList(), 1)
    }

    override fun valAt(key: Any?) = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = vectorForOrNull(key as String) ?: notFound

    override fun seq(): ISeq? = RT.seq(vectors)
    override fun count() = vectors.count()
}
