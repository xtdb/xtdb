package xtdb.arrow

import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

private const val ROWS = 200

@ExtendWith(AllocatorResolver::class)
class MaskedRelationTest {

    private fun BufferAllocator.openRel(): RelationReader =
        RelationReader.from(
            listOf(
                Vector.fromList(this, "a", List(ROWS) { it.toLong() }),
                Vector.fromList(this, "b", List(ROWS) { if (it % 7 == 0) null else "s$it" }),
            ),
            ROWS
        )

    @Test
    fun `stacked masks refine to one mask over the unmasked rows`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val once = MaskedRelation.of(rel, rowMask(ROWS) { it % 3 != 1 })
            val twice = MaskedRelation.of(once, rowMask(once.rowCount) { it % 2 == 0 || it in 40..45 })
            val expected = (0 until ROWS).filter { it % 3 != 1 }.filterIndexed { k, _ -> k % 2 == 0 || k in 40..45 }

            twice.mask.rowCount shouldBe ROWS
            twice.mask.toSelection().toList() shouldBe expected

            twice.rowCount shouldBe expected.size
            twice.vectors.all { it is MaskedVector } shouldBe true
            twice.asMaps shouldBe rel.select(expected.toIntArray()).asMaps
        }
    }

    @Test
    fun `select resolves to the unmasked rows in one gather`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val mask = rowMask(ROWS) { it % 3 == 0 || it in 60..70 }
            val masked = MaskedRelation.of(rel, mask)
            val expected = rel.select(mask.toSelection())

            val idxs = intArrayOf(masked.rowCount - 1, 0, 21, 21, 22)
            masked.select(idxs).let { sel ->
                sel.asMaps shouldBe expected.select(idxs).asMaps
                sel.vectors.none { it is MaskedVector } shouldBe true
            }

            masked.select(20, 15).let { sel ->
                sel.asMaps shouldBe expected.select(20, 15).asMaps
                sel.vectors.none { it is MaskedVector } shouldBe true
            }
        }
    }

    @Test
    fun `a slice outlives the relation it was sliced from`(al: BufferAllocator) {
        val mask = rowMask(ROWS) { it % 4 == 1 }
        val rel = al.openRel()
        val expected = rel.select(mask.toSelection()).asMaps

        MaskedRelation.of(rel, mask).use { masked ->
            masked.openSlice(al)
        }.use { slice ->
            slice.rowCount shouldBe mask.cardinality
            slice.asMaps shouldBe expected
        }
    }
}
