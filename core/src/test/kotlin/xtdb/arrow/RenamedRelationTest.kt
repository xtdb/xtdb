package xtdb.arrow

import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.Vector.Companion.openVector
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class RenamedRelationTest {

    private fun BufferAllocator.openRel() =
        RelationReader.from(mapOf("a" to Vector.fromList(this, listOf(1L, 2L, 3L))), 3)

    private fun RelationReader.renamedTo(name: String) = RelationReader.from(mapOf(name to cols.values.single()), rowCount)

    private val RelationReader.colNames get() = schema.fields.map { it.name }

    @Test
    fun `a renamed column reports its new name through schema`(al: BufferAllocator) {
        al.openRel().use { rel ->
            rel.renamedTo("b").colNames shouldBe listOf("b")
        }
    }

    @Test
    fun `a renamed column's rows are keyed by its new name`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val renamed = rel.renamedTo("b")
            renamed.asMaps.map { it.keys.single().toString() } shouldBe listOf(":b", ":b", ":b")
            renamed[1].keys.single().toString() shouldBe ":b"
        }
    }

    @Test
    fun `selections and slices of a renamed relation keep its names`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val renamed = rel.renamedTo("b")
            renamed.select(intArrayOf(0, 2)).colNames shouldBe listOf("b")
            renamed.openSlice(al).use { it.colNames shouldBe listOf("b") }
            renamed.openDirectSlice(al).use { it.colNames shouldBe listOf("b") }
            MaskedRelation.of(renamed, RowMask.all(3)).colNames shouldBe listOf("b")
        }
    }

    @Test
    fun `copying a renamed relation writes its new names`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val renamed = rel.renamedTo("b")

            Relation(al).use { dest ->
                dest.append(renamed)
                dest.colNames shouldBe listOf("b")
            }

            Relation(al).use { dest ->
                renamed.rowCopier(dest).copyRange(0, 3)
                dest.colNames shouldBe listOf("b")
                dest["b"].getObject(2) shouldBe 3L
            }
        }
    }

    @Test
    fun `a renamed relation as a struct names its children by the new names`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val renamed = rel.renamedTo("b")

            renamed.openAsStruct(al).use { it.keyNames shouldBe setOf("b") }

            al.openVector(structOf("b" to I64)).use { dest ->
                dest.appendStructRows(renamed)
                dest.keyNames shouldBe setOf("b")
                dest.getObject(0) shouldBe mapOf("b" to 1L)
            }
        }
    }

    @Test
    fun `renaming a renamed relation goes by the relation's names`(al: BufferAllocator) {
        al.openRel().use { rel ->
            val twice = rel.renamedTo("b").let { r -> RelationReader.from(r.cols.mapKeys { "c" }, r.rowCount) }
            twice.colNames shouldBe listOf("c")
            twice.vectorFor("c").getObject(1) shouldBe 2L
        }
    }
}
