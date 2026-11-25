package xtdb.arrow

import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorIndirection.Mask
import xtdb.test.AllocatorResolver

private const val ROWS = 200

@ExtendWith(AllocatorResolver::class)
class IndirectRelationTest {

    private fun BufferAllocator.openRel(): RelationReader =
        RelationReader.from(
            mapOf(
                "a" to Vector.fromList(this, List(ROWS) { it.toLong() }),
                "b" to Vector.fromList(this, List(ROWS) { if (it % 7 == 0) null else "s$it" }),
            ),
            ROWS
        )

    @Test
    fun `stacked masks refine to one mask over the unmasked rows`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val once = rel.select(rowMask(ROWS) { it % 3 != 1 })
            val twice = once.select(rowMask(once.rowCount) { it % 2 == 0 || it in 40..45 }) as IndirectRelation
            val expected = (0 until ROWS).filter { it % 3 != 1 }.filterIndexed { k, _ -> k % 2 == 0 || k in 40..45 }

            val mask = twice.sel as Mask
            mask.rowCount shouldBe ROWS
            mask.toSelection().toList() shouldBe expected

            twice.rowCount shouldBe expected.size
            twice.cols.values.all { it is IndirectVector && it.sel === mask } shouldBe true
            twice.asMaps shouldBe rel.select(expected.toIntArray()).asMaps
        }
    }

    @Test
    fun `a mask over a selected relation's columns is one selection over each base column`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val idxs = IntArray(ROWS / 2) { ROWS - 1 - 2 * it }
            val mask = rowMask(idxs.size) { it % 3 != 0 }
            val expected = idxs.filterIndexed { k, _ -> k % 3 != 0 }.toIntArray()

            rel.select(idxs).select(mask).let { masked ->
                masked.cols.values.all { it is IndirectVector && it.sel == selection(expected) } shouldBe true
                masked.asMaps shouldBe rel.select(expected).asMaps
            }
        }
    }

    @Test
    fun `select resolves to the unmasked rows in one gather`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val mask = rowMask(ROWS) { it % 3 == 0 || it in 60..70 }
            val masked = rel.select(mask)
            val expected = rel.select(mask.toSelection())

            val idxs = intArrayOf(masked.rowCount - 1, 0, 21, 21, 22)
            masked.select(idxs).let { sel ->
                sel.asMaps shouldBe expected.select(idxs).asMaps
                sel.cols.values.none { (it as? IndirectVector)?.sel is Mask } shouldBe true
            }
        }
    }

    @Test
    fun `a selection over a selected relation is one selection over the base relation`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val idxs = IntArray(ROWS / 2) { ROWS - 1 - 2 * it }
            val idxs2 = intArrayOf(idxs.size - 1, 0, 7, 7, 30)
            val expected = IntArray(idxs2.size) { idxs[idxs2[it]] }

            (rel.select(idxs).select(idxs2) as IndirectRelation).let { selected ->
                selected.sel shouldBe selection(expected)
                selected.cols.values.all { it is IndirectVector && it.sel == selection(expected) } shouldBe true
                selected.asMaps shouldBe rel.asMaps.let { maps -> expected.map { maps[it] } }
            }
        }
    }

    @Test
    fun `a slice outlives the relation it was sliced from`(al: BufferAllocator) {
        val mask = rowMask(ROWS) { it % 4 == 1 }
        val rel = al.openRel()
        val expected = rel.select(mask.toSelection()).asMaps

        rel.select(mask).use { masked ->
            masked.openSlice(al)
        }.use { slice ->
            slice.rowCount shouldBe mask.cardinality
            slice.asMaps shouldBe expected
        }
    }
}
