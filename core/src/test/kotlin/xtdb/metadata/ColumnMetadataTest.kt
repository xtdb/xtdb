package xtdb.metadata

import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.Relation
import xtdb.arrow.StructVector
import xtdb.arrow.VectorReader
import xtdb.test.AllocatorResolver
import xtdb.trie.MetadataFileWriter

@ExtendWith(AllocatorResolver::class)
class ColumnMetadataTest {

    private data class Stats(val count: Long, val min: Double?, val max: Double?)

    private fun childStats(al: BufferAllocator, docs: List<Map<String, Any>?>, childName: String): Stats =
        StructVector(al, "struct", true).use { structVec ->
            docs.forEach { structVec.writeObject(it) }

            Relation(al, MetadataFileWriter.metaRelSchema).use { metaRel ->
                val colsVec = metaRel["nodes"]["leaf"]["columns"].listElements
                ColumnMetadata(colsVec, false).writeMetadata(structVec)

                val idx = (0 until colsVec.valueCount).single { colsVec["col-name"].getObject(it) == childName }
                val numbers: VectorReader? = colsVec.vectorForOrNull("numbers")

                Stats(
                    colsVec["count"].getLong(idx),
                    numbers?.takeUnless { it.isNull(idx) }?.let { it["min"].getDouble(idx) },
                    numbers?.takeUnless { it.isNull(idx) }?.let { it["max"].getDouble(idx) },
                )
            }
        }

    @Test
    fun `a struct child's metadata leaves out the rows where the struct is null`(al: BufferAllocator) {
        childStats(al, listOf(mapOf("a" to 5L), null, mapOf("a" to 7L)), "a") shouldBe Stats(2, 5.0, 7.0)
    }

    @Test
    fun `a struct child promoted to a union leaves out the rows where the struct is null`(al: BufferAllocator) {
        childStats(al, listOf(mapOf("a" to 1L), null, mapOf("a" to true)), "a") shouldBe Stats(2, 1.0, 1.0)
    }

    @Test
    fun `a nested struct's child leaves out the rows where an outer struct is null`(al: BufferAllocator) {
        val docs = listOf(mapOf("s" to mapOf("a" to 5L)), null, mapOf("s" to mapOf("a" to 7L)))
        childStats(al, docs, "a") shouldBe Stats(2, 5.0, 7.0)
    }
}
