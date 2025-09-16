package xtdb.expression.map

import org.apache.arrow.memory.util.hash.MurmurHasher
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorWriter
import xtdb.trie.ColumnName
import xtdb.util.Hasher

class IndexHasher(private val rel: RelationReader, private val hashCols: List<VectorReader>) {
    private val hasher = Hasher.Xx()

    fun hashCode(index: Int): Int =
        hashCols.foldRight(0) { col, acc ->
            MurmurHasher.combineHashCode(
                acc,
                col.hashCode(index, hasher)
            )
        }

    fun writeAllHashes(hashCol: VectorWriter) {
        hashCol.clear()

        repeat(rel.rowCount) { idx ->
            hashCol.writeInt(hashCode(idx))
        }
    }

    companion object {
        fun RelationReader.hasher(hashColNames: List<ColumnName>? = null) =
            IndexHasher(this, hashColNames?.map { this[it] } ?: vectors.toList())
    }
}