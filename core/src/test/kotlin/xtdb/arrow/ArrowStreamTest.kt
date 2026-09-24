package xtdb.arrow

import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver
import java.nio.channels.Channels
import java.util.UUID

@ExtendWith(AllocatorResolver::class)
class ArrowStreamTest {

    private fun BufferAllocator.reload(rel: RelationReader, startIdx: Int, len: Int) =
        Relation.StreamLoader(this, Channels.newChannel(rel.toArrowStream(startIdx, len).newInput())).use { loader ->
            Relation(this, loader.schema).use { out ->
                loader.loadNextPage(out) shouldBe true
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

    private fun assertMatchesArrowsWriter(rel: Relation) =
        assertArrayEquals(rel.asArrowStream, rel.toArrowStream().toByteArray())

    // 20 rows takes every start past a byte boundary, which is where validity has to be shifted rather than copied
    private fun Relation.writeTwentyRows(row: (Int) -> Map<*, *>) = apply {
        repeat(20) { writeRow(row(it)) }
    }

    private fun Relation.writeMixedRows() = writeTwentyRows { idx ->
        mapOf(
            "i64" to idx.toLong(),
            "utf8" to if (idx % 3 == 0) null else "row-$idx".repeat(idx % 4 + 1),
            "bool" to (idx % 2 == 0),
            "nil" to null,
            "uuid" to UUID(0, idx.toLong()),
            "s" to if (idx % 5 == 0) null else mapOf("a" to idx.toLong(), "b" to if (idx % 2 == 0) null else "b-$idx"),
            "l" to if (idx % 7 == 0) null else List(idx % 3) { (idx * 10 + it).toLong() },
            "duv" to when (idx % 4) {
                0 -> idx.toLong()
                1 -> "utf8-$idx"
                2 -> null
                else -> idx % 2 == 0
            },
        )
    }

    private val fixedSizeListField =
        Field(
            "fsl", FieldType.nullable(ArrowType.FixedSizeList(2)),
            listOf(Field("\$data\$", FieldType.notNullable(ArrowType.Int(64, true)), null))
        )

    private val mapField =
        Field(
            "m", FieldType.nullable(ArrowType.Map(false)),
            listOf(
                Field(
                    "entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                    listOf(
                        Field("key", FieldType.notNullable(ArrowType.Int(32, true)), null),
                        Field("value", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                    )
                )
            )
        )

    private fun Relation.writeNestedRows() = writeTwentyRows { idx ->
        mapOf(
            "fsl" to if (idx % 6 == 0) null else listOf(idx.toLong(), -idx.toLong()),
            "m" to if (idx % 4 == 0) null else mapOf(idx to "v-$idx", idx + 100 to null),
        )
    }

    @Test
    fun `a whole relation's stream is byte-for-byte what Arrow's own writer produces`(al: BufferAllocator) {
        Relation(al).use { rel ->
            rel.writeMixedRows()
            rel["duv"].legNames shouldBe setOf("i64", "utf8", "null", "bool")
            assertMatchesArrowsWriter(rel)
        }

        Relation(al, fixedSizeListField, mapField).use { rel ->
            rel.writeNestedRows()
            assertMatchesArrowsWriter(rel)
        }
    }

    @Test
    fun `an empty relation's stream matches Arrow's own writer`(al: BufferAllocator) {
        Relation(al).use { rel -> assertMatchesArrowsWriter(rel) }

        Relation(al, fixedSizeListField, mapField).use { rel -> assertMatchesArrowsWriter(rel) }
    }

    @Test
    fun `every row range reads back as those rows alone`(al: BufferAllocator) {
        Relation(al).use { rel ->
            rel.writeMixedRows()
            al.assertEveryRangeReloads(rel)
        }

        Relation(al, fixedSizeListField, mapField).use { rel ->
            rel.writeNestedRows()
            al.assertEveryRangeReloads(rel)
        }
    }

    @Test
    fun `a stream outlives the relation it was written from`(al: BufferAllocator) {
        val stream = Relation(al).use { rel ->
            rel.writeTwentyRows { idx -> mapOf("i64" to idx.toLong(), "utf8" to "row-$idx") }
            rel.toArrowStream(4, 9)
        }

        Relation.StreamLoader(al, Channels.newChannel(stream.newInput())).use { loader ->
            Relation(al, loader.schema).use { out ->
                loader.loadNextPage(out)
                out["i64"].asList shouldBe (4L..12L).toList()
            }
        }
    }

    @Test
    fun `a selection cannot be written as a stream`(al: BufferAllocator) {
        Relation(al).use { rel ->
            rel.writeTwentyRows { idx -> mapOf("i64" to idx.toLong()) }

            assertThrows<UnsupportedOperationException> {
                rel.select(intArrayOf(3, 1, 2)).toArrowStream(0, 3)
            }
        }
    }
}
