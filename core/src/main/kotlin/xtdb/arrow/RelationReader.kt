package xtdb.arrow

import org.apache.arrow.vector.types.pojo.Schema
import java.util.*
import xtdb.vector.RelationReader as OldRelationReader

interface RelationReader : Iterable<VectorReader>, AutoCloseable {
    val schema: Schema
    val rowCount: Int

    operator fun get(colName: String): VectorReader?

    private class IndirectRelation(
        private val vecs: SequencedMap<String, VectorReader>,
        override val rowCount: Int
    ) : RelationReader {
        override val schema get() = Schema(vecs.map { it.value.field })

        override fun get(colName: String) = vecs[colName]

        override fun iterator() = vecs.values.iterator()

        override fun close() = vecs.forEach { it.value.close() }
    }

    fun select(idxs: IntArray): RelationReader =
        IndirectRelation(associateTo(linkedMapOf()) { it.name to it.select(idxs) }, idxs.size)

    override fun close() = forEach { it.close() }

    companion object {
        @JvmStatic
        fun from(oldReader: OldRelationReader) = object : RelationReader {
            override val schema = Schema(oldReader.map { it.field })
            override val rowCount: Int get() = oldReader.rowCount()

            override operator fun get(colName: String) =
                oldReader.readerForName(colName)?.let { VectorReader.from(it) }

            override fun iterator() = oldReader.asSequence().map { VectorReader.from(it) }.iterator()
        }
    }

}
