package xtdb.arrow

import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.kw
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class RangedUnloadTest {

    private fun BufferAllocator.reload(rel: Relation, startIdx: Int, len: Int) =
        rel.openArrowRecordBatch(startIdx, len).use { batch ->
            Relation(this, rel.schema).use { out ->
                out.load(batch)
                out.rowCount shouldBe len
                out.asMaps
            }
        }

    private fun BufferAllocator.assertEveryRangeReloads(rel: Relation) {
        for (startIdx in 0..rel.rowCount) {
            for (len in 0..rel.rowCount - startIdx) {
                withClue("rows [$startIdx, ${startIdx + len})") {
                    reload(rel, startIdx, len) shouldBe rel.select(startIdx, len).asMaps
                }
            }
        }
    }

    // 20 rows takes every start past a byte boundary, which is where validity has to be shifted rather than sliced
    private fun Relation.writeTwentyRows(row: (Int) -> Map<*, *>) = apply {
        repeat(20) { writeRow(row(it)) }
    }

    @Test
    fun `fixed and variable width, with nulls`(al: BufferAllocator) {
        Relation(al).use { rel ->
            rel.writeTwentyRows { idx ->
                mapOf(
                    "i64" to idx.toLong(),
                    "utf8" to if (idx % 3 == 0) null else "row-$idx".repeat(idx % 4 + 1),
                    "bool" to (idx % 2 == 0)
                )
            }

            al.assertEveryRangeReloads(rel)
        }
    }

    @Test
    fun `struct children take their parent's range`(al: BufferAllocator) {
        Relation(al).use { rel ->
            rel.writeTwentyRows { idx ->
                mapOf(
                    "s" to if (idx % 5 == 0) null
                    else mapOf("a" to idx.toLong(), "b" to if (idx % 2 == 0) null else "b-$idx")
                )
            }

            al.assertEveryRangeReloads(rel)
        }
    }

    @Test
    fun `a union's legs take their own sub-ranges`(al: BufferAllocator) {
        Relation(al).use { rel ->
            rel.writeTwentyRows { idx ->
                mapOf(
                    "duv" to when (idx % 4) {
                        0 -> idx.toLong()
                        1 -> "utf8-$idx"
                        2 -> null
                        else -> idx % 2 == 0
                    }
                )
            }

            rel["duv"].legNames shouldBe setOf("i64", "utf8", "null", "bool")

            al.assertEveryRangeReloads(rel)
        }
    }

    @Test
    fun `list elements take the range their offsets give`(al: BufferAllocator) {
        Relation(al).use { rel ->
            rel.writeTwentyRows { idx ->
                mapOf("l" to if (idx % 7 == 0) null else List(idx % 3) { (idx * 10 + it).toLong() })
            }

            al.assertEveryRangeReloads(rel)
        }
    }

    @Test
    fun `a record batch outlives the relation it was unloaded from`(al: BufferAllocator) {
        lateinit var schema: Schema

        val batch = Relation(al).use { rel ->
            rel.writeTwentyRows { idx -> mapOf("i64" to idx.toLong(), "utf8" to "row-$idx") }
            schema = rel.schema
            rel.openArrowRecordBatch(4, 9)
        }

        batch.use {
            Relation(al, schema).use { out ->
                out.load(it)
                out.asMaps.map { row -> row["i64".kw] } shouldBe (4L..12L).toList()
            }
        }
    }

    @Test
    fun `a selection cannot be unloaded`(al: BufferAllocator) {
        Relation(al).use { rel ->
            rel.writeTwentyRows { idx -> mapOf("i64" to idx.toLong()) }

            assertThrows<UnsupportedOperationException> {
                rel.select(intArrayOf(3, 1, 2)).openArrowRecordBatch(0, 3)
            }
        }
    }
}
