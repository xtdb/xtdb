package xtdb.arrow

import clojure.lang.PersistentHashMap
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
import java.util.*
import xtdb.vector.RelationReader as OldRelationReader

interface RelationReader<V : VectorReader> : Iterable<V>, AutoCloseable {
    val schema: Schema
    val rowCount: Int

    operator fun get(colName: String): V

    private class IndirectRelation(
        private val vecs: SequencedMap<String, VectorReader>,
        override val rowCount: Int
    ) : RelationReader<VectorReader> {
        override val schema get() = Schema(vecs.map { it.value.field })

        override fun get(colName: String) = vecs[colName] ?: error("missing col: $colName")

        override fun iterator() = vecs.values.iterator()

        override fun close() = vecs.forEach { it.value.close() }
    }

    fun select(idxs: IntArray): RelationReader<*> =
        IndirectRelation(associateTo(linkedMapOf()) { it.name to it.select(idxs) }, idxs.size)

    override fun close() = forEach { it.close() }

    fun toTuples() = toTuples(KEBAB_CASE_KEYWORD)

    @Suppress("unused")
    fun toTuples(keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD) =
        (0..<rowCount).map { idx -> map { it.getObject(idx, keyFn) } }

    fun toMaps() = toMaps(KEBAB_CASE_KEYWORD)

    @Suppress("unused")
    fun toMaps(keyFn: IKeyFn<*> = KEBAB_CASE_KEYWORD) =
        (0..<rowCount).map { idx ->
            PersistentHashMap.create(
                associate {
                    Pair(
                        keyFn.denormalize(it.name),
                        it.getObject(idx, keyFn)
                    )
                }
            ) as Map<*, *>
        }

    companion object {
        private class FromOldRelation(private val oldReader: OldRelationReader) :
            RelationReader<VectorReader> {
            override val schema = Schema(oldReader.map { it.field })
            override val rowCount: Int get() = oldReader.rowCount()

            override operator fun get(colName: String): VectorReader =
                oldReader.readerForName(colName)!!.let { VectorReader.from(it) }

            override fun iterator() = oldReader.asSequence().map { VectorReader.from(it) }.iterator()
        }

        @JvmStatic
        fun from(oldReader: OldRelationReader): RelationReader<*> = FromOldRelation(oldReader)

        class FromCols(
            private val cols: SequencedMap<String, VectorReader>, override val rowCount: Int
        ) : RelationReader<VectorReader> {
            override val schema get() = Schema(cols.values.map { it.field })

            override fun get(colName: String) = cols[colName] ?: error("missing column: $colName")

            override fun iterator() = cols.values.iterator()
        }

        fun from(cols: Iterable<VectorReader>, rowCount: Int) =
            FromCols(cols.associateByTo(linkedMapOf()) { it.name }, rowCount)
    }

    val oldRelReader: xtdb.vector.RelationReader
        get() = OldRelationReader.from(map(VectorReader.Companion::NewToOldAdapter), rowCount)
}
