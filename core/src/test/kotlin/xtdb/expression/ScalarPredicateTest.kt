package xtdb.expression

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.CmpOp
import xtdb.arrow.IndirectVector
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.VectorIndirection
import xtdb.arrow.VectorIndirection.Mask
import xtdb.expression.ScalarPredicate.Bound
import xtdb.expression.ScalarPredicate.Literal
import xtdb.test.AllocatorResolver

private const val ROWS = 150

@ExtendWith(AllocatorResolver::class)
class ScalarPredicateTest {

    private fun rowMask(n: Int, set: (Int) -> Boolean) = Mask.Builder(n).apply { (0 until n).filter(set).forEach(::add) }.build()

    private fun Mask?.rows() = this?.let { it.rowCount to it.toSelection().toList() }

    private val masks: Map<String, (Int) -> Boolean> = mapOf(
        "either side of each word boundary" to { it in setOf(0, 62, 63, 64, 65, 127, 128, 149) },
        "every third row" to { it % 3 == 0 },
        "all but the first and last" to { it != 0 && it != ROWS - 1 },
        "the last row only" to { it == ROWS - 1 },
        "all" to { true },
        "none" to { false },
    )

    private val i64Bounds = listOf(
        Bound.Compare("x", CmpOp.LT, Literal.Int64(640)),
        Bound.Compare("x", CmpOp.GTE, Literal.Int64(25)),
        Bound.Compare("x", CmpOp.EQ, Literal.Int64(320)),
        Bound.Compare("x", CmpOp.GT, Literal.Float64(100.5)),
    )

    private fun checkMaskedAgreesWithSelection(vec: Vector, bounds: List<Bound>) {
        val rel = RelationReader.from(mapOf("x" to vec), ROWS)

        masks.forEach { (maskLabel, set) ->
            val mask = rowMask(ROWS, set)
            val masked = rel.select(mask)
            val selected = rel.select(mask.toSelection())
            val n = masked.rowCount

            for ((inputLabel, input) in mapOf(
                "all rows" to Mask.all(n),
                "every other row" to rowMask(n) { it % 2 == 0 },
                "no rows" to rowMask(n) { false },
            )) {
                for (bound in bounds) {
                    val expected = bound.select(selected, input)
                    assertEquals(true, expected != null, "$bound, $maskLabel, $inputLabel: the selection answers")
                    assertEquals(expected.rows(), bound.select(masked, input).rows(), "$bound, $maskLabel, $inputLabel")
                }
            }
        }
    }

    @Test
    fun `comparisons through a mask agree with the selection of the mask's rows`(al: BufferAllocator) {
        Vector.fromList(al, List(ROWS) { if (it % 7 == 3) null else it * 10L }).use { vec ->
            checkMaskedAgreesWithSelection(vec, i64Bounds)
        }
    }

    @Test
    fun `a column that is no view of a vector the comparison covers isn't answered`(al: BufferAllocator) {
        Vector.fromList(al, List(ROWS) { if (it % 7 == 3) null else it * 10L }).use { vec ->
            val nested = IndirectVector(IndirectVector(vec, VectorIndirection.selection(IntArray(ROWS) { it })), rowMask(ROWS) { it % 3 == 0 })
            val nestedRel = RelationReader.from(mapOf("x" to nested))
            for (bound in i64Bounds) assertEquals(null, bound.select(nestedRel, Mask.all(nestedRel.rowCount)), "$bound, nested view")
        }

        Vector.fromList(al, List(ROWS) { if (it % 6 == 0) null else listOf(it, it + 1) }).use { vec ->
            val masked = RelationReader.from(mapOf("x" to vec), ROWS).select(rowMask(ROWS) { it % 3 == 0 })
            for (bound in i64Bounds) assertEquals(null, bound.select(masked, Mask.all(masked.rowCount)), "$bound, masked list")
        }
    }
}
