package xtdb.table

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorType

class ColumnOrdinalsTest {

    private fun col(name: String, type: VectorType) = mapOf(name to ColumnMeta.of(name, type))

    private fun struct(vararg children: Pair<String, VectorType>) = VectorType.Struct(children.toMap())

    private val ColumnMeta.Type.structOrdinals: Map<String, Int>
        get() = when (this) {
            is ColumnMeta.Type.Struct -> children.mapValues { it.value.ordinal }
            is ColumnMeta.Type.Maybe -> mono.structOrdinals
            is ColumnMeta.Type.Poly -> legs.filterIsInstance<ColumnMeta.Type.Struct>().single().structOrdinals
            else -> error("no struct at this position: $this")
        }

    private fun Map<String, ColumnMeta>.ordinalsOf(col: String) = getValue(col).type.structOrdinals

    private val zOnly = col("s", struct("z" to VectorType.I64)).withOrdinalsFrom(emptyMap())

    private val zThenA =
        col("s", struct("z" to VectorType.I64, "a" to VectorType.I64)).withOrdinalsFrom(zOnly)

    @Test
    fun `a struct child added later takes the next ordinal rather than its place in name order`() {
        assertEquals(mapOf("z" to 0, "a" to 1), zThenA.ordinalsOf("s"))
    }

    @Test
    fun `a struct child keeps its ordinal when its column becomes nullable`() {
        val nullable = col("s", VectorType.maybe(struct("z" to VectorType.I64, "a" to VectorType.I64)))
            .withOrdinalsFrom(zThenA)

        assertEquals(mapOf("z" to 0, "a" to 1), nullable.ordinalsOf("s"))
    }

    @Test
    fun `a struct child keeps its ordinal when its column takes a second leg`() {
        val union = col("s", VectorType.fromLegs(struct("z" to VectorType.I64, "a" to VectorType.I64), VectorType.I64))
            .withOrdinalsFrom(zThenA)

        assertEquals(mapOf("z" to 0, "a" to 1), union.ordinalsOf("s"))
    }

    @Test
    fun `a struct child keeps its ordinal when its column narrows back`() {
        val nullable = col("s", VectorType.maybe(struct("z" to VectorType.I64, "a" to VectorType.I64)))
            .withOrdinalsFrom(zThenA)

        val narrowed = col("s", struct("z" to VectorType.I64, "a" to VectorType.I64)).withOrdinalsFrom(nullable)

        assertEquals(mapOf("z" to 0, "a" to 1), narrowed.ordinalsOf("s"))
    }
}
