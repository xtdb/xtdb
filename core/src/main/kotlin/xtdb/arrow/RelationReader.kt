package xtdb.arrow

import xtdb.vector.RelationReader as OldRelationReader

interface RelationReader : Iterable<VectorReader>, AutoCloseable {
    val rowCount: Int

    operator fun get(colName: String): VectorReader?

    override fun close() = forEach { it.close() }

    companion object {
        @JvmStatic
        fun from(oldReader: OldRelationReader) = object : RelationReader {
            override val rowCount: Int get() = oldReader.rowCount()

            override operator fun get(colName: String) =
                oldReader.readerForName(colName)?.let { VectorReader.from(it) }

            override fun iterator() = oldReader.asSequence().map { VectorReader.from(it) }.iterator()
        }
    }

}
