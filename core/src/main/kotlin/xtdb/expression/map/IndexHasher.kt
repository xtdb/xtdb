package xtdb.expression.map

import org.apache.arrow.memory.util.hash.MurmurHasher
import xtdb.arrow.VectorReader
import xtdb.util.Hasher

fun interface IndexHasher {
    fun hashCode(index: Int): Int

    companion object {
        fun fromCols(cols: List<VectorReader>): IndexHasher {
            val hasher = Hasher.Xx()

            return IndexHasher { index ->
                cols.foldRight(0) { col, acc ->
                    MurmurHasher.combineHashCode(
                        acc,
                        col.hashCode(index, hasher)
                    )
                }
            }
        }
    }
}