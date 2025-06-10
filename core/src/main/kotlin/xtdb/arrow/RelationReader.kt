package xtdb.arrow

import clojure.lang.Counted
import clojure.lang.ILookup
import clojure.lang.ISeq
import clojure.lang.PersistentHashMap
import clojure.lang.RT
import clojure.lang.Seqable
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
import xtdb.util.closeAll
import java.util.*
import xtdb.vector.RelationReader as OldRelationReader

interface RelationReader : ILookup, Seqable, Counted, AutoCloseable {
    val schema: Schema
    val rowCount: Int

    val vectors: Iterable<VectorReader>

    fun vectorForOrNull(name: String): VectorReader?
    fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    operator fun get(name: String) = vectorFor(name)

    operator fun get(idx: Int, keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD): Map<*, Any?> =
        vectors.associate { keyFn.denormalize(it.name) to it.getObject(idx, keyFn) }

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
                vectors.associate {
                    Pair(
                        keyFn.denormalize(it.name),
                        it.getObject(idx, keyFn)
                    )
                }
            ) as Map<*, *>
        }

    class FromCols(private val cols: SequencedMap<String, VectorReader>, override val rowCount: Int) : RelationReader {
        override val schema get() = Schema(cols.values.map { it.field })

        override fun vectorForOrNull(name: String) = cols[name]
        override val vectors get() = cols.values

        override fun select(idxs: IntArray): RelationReader =
            FromCols(cols.entries.associateTo(linkedMapOf()) { it.key to it.value.select(idxs) }, idxs.size)
    }

    companion object {
        private class FromOldRelation(private val oldReader: OldRelationReader) : RelationReader {
            override val schema = Schema(oldReader.vectors.map { it.field })
            override val rowCount: Int get() = oldReader.rowCount
            override val vectors get() = oldReader.vectors.map { VectorReader.from(it) }

            override fun vectorForOrNull(name: String): VectorReader? =
                oldReader.vectorForOrNull(name)?.let { VectorReader.from(it) }

            override fun select(idxs: IntArray): RelationReader = FromOldRelation(oldReader.select(idxs))
        }

        @JvmStatic
        fun from(oldReader: OldRelationReader): RelationReader = FromOldRelation(oldReader)

        fun from(cols: Iterable<VectorReader>, rowCount: Int): RelationReader =
            FromCols(cols.associateByTo(linkedMapOf()) { it.name }, rowCount)
    }

    override fun valAt(key: Any?) = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = vectorForOrNull(key as String) ?: notFound

    override fun seq(): ISeq? = RT.seq(vectors)
    override fun count() = vectors.count()
}
