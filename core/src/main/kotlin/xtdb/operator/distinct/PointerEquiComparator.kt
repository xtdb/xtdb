package xtdb.operator.distinct

import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.VectorReader
import java.util.function.IntBinaryOperator

/**
 * dart(#6285 group-by keys): allocation-free equality for variable-width keys -
 * compares the two values' bytes in place via [ArrowBufPointer] rather than reading each into a `ByteBuffer`.
 * Same 1 / -1 / 0 (null) contract as the generated `==` / `null_eq` comparators.
 */
object PointerEquiComparator {
    private val ENABLED = System.getProperty("xtdb.ptrEqui", "true") == "true"

    private fun isVarWidth(v: VectorReader) = v.arrowType.let { it is ArrowType.Utf8 || it is ArrowType.Binary }

    @JvmStatic
    fun orNull(l: VectorReader, r: VectorReader, nilKeysEqual: Boolean): IntBinaryOperator? {
        if (!ENABLED || !isVarWidth(l) || !isVarWidth(r) || l.arrowType != r.arrowType) return null
        val lp = ArrowBufPointer()
        val rp = ArrowBufPointer()
        val lNullable = l.nullable
        val rNullable = r.nullable

        return IntBinaryOperator { li, ri ->
            val ln = lNullable && l.isNull(li)
            val rn = rNullable && r.isNull(ri)
            when {
                ln || rn -> if (nilKeysEqual) (if (ln && rn) 1 else -1) else 0
                l.getPointer(li, lp) == r.getPointer(ri, rp) -> 1
                else -> -1
            }
        }
    }
}
