package xtdb.expression.map

import org.apache.arrow.memory.util.hash.MurmurHasher.combineHashCode
import xtdb.arrow.IntVector
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.trie.ColumnName
import xtdb.util.Hasher

class IndexHasher(private val rel: RelationReader, private val hashCols: List<VectorReader>) {
    /**
     * Replaces [out]'s contents with every row's key hash - a [combineHashCode] fold, from 0, over the row's key-column xxHashes - hashing a key column at a time.
     * [out] and [scratch] must be non-nullable; a multi-column key overwrites [scratch] with each further column's hashes in turn.
     */
    fun hashTo(out: IntVector, scratch: IntVector) {
        out.clear()

        val firstCol = hashCols.firstOrNull()
            ?: return out.writeInts(rel.rowCount) { 0 }

        val hasher = Hasher.Xx()
        firstCol.hashTo(out, hasher)

        if (hashCols.size == 1)
            return out.mapInts { _, h -> combineHashCode(0, h) }

        for (colIdx in 1 until hashCols.size) {
            scratch.clear()
            hashCols[colIdx].hashTo(scratch, hasher)

            if (colIdx == 1)
                out.mapInts { idx, h -> combineHashCode(combineHashCode(0, h), scratch.getInt(idx)) }
            else
                out.mapInts { idx, acc -> combineHashCode(acc, scratch.getInt(idx)) }
        }
    }

    companion object {
        fun RelationReader.hasher(hashColNames: List<ColumnName>? = null) =
            IndexHasher(this, hashColNames?.map { this[it] } ?: vectors.toList())
    }
}
