package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.VectorUnloader
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.ValueVectorUtility
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver
import java.time.ZonedDateTime
import java.util.UUID

@ExtendWith(AllocatorResolver::class)
class ConformingUnloadTest {

    private fun RelationReader.fieldOrAbsent(name: String) =
        vectorForOrNull(name)?.field ?: Field(name, FieldType.nullable(NULL_TYPE), null)

    // the declared type of each column across both pages, as XTDB's own type merge has it
    private fun mergedSchema(a: RelationReader, b: RelationReader) =
        Schema((a.vectors + b.vectors).map { it.name }.distinct().map { name ->
            MergeFields.mergeFields(listOf(a.fieldOrAbsent(name), b.fieldOrAbsent(name)))
                .let { Field(name, it.fieldType, it.children) }
        })

    private fun assertUnloadsAs(al: BufferAllocator, page: RelationReader, schema: Schema) {
        page.openArrowRecordBatch(al, schema).use { rb ->
            VectorSchemaRoot.create(Schema(schema.fields.map { it.withUnionTypeIds() }), al).use { root ->
                VectorLoader(root).load(rb)
                ValueVectorUtility.validateFull(root)

                // what the Flight SQL server does with each buffer of a batch it streams
                VectorUnloader(root).recordBatch.use { it.buffers.forEach { buf -> buf.nioBuffer() } }

                Relation.fromRoot(al, root).use { assertEquals(page.asMaps, it.asMaps) }
            }
        }
    }

    private fun assertBothUnloadAsMerged(al: BufferAllocator, aRows: List<Map<String, *>>, bRows: List<Map<String, *>>) {
        Relation.openFromRows(al, aRows).use { a ->
            Relation.openFromRows(al, bRows).use { b ->
                val schema = mergedSchema(a, b)
                assertUnloadsAs(al, a, schema)
                assertUnloadsAs(al, b, schema)
                assertUnloadsAs(al, a, a.schema)
            }
        }
    }

    @Test
    fun `a struct field one page lacks`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("vq" to mapOf("value" to 1.0, "unit" to "mg"))), listOf(mapOf("vq" to mapOf("value" to 2.0))))

    @Test
    fun `a scalar that's a long on one page and a string on the other`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("v" to 1L)), listOf(mapOf("v" to "str")))

    @Test
    fun `a top-level column one page lacks`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("a" to 1L, "extra" to "x")), listOf(mapOf("a" to 2L)))

    @Test
    fun `a struct in a list in a struct`(al: BufferAllocator) =
        assertBothUnloadAsMerged(
            al,
            listOf(mapOf("r" to mapOf("items" to listOf(mapOf("c" to mapOf("code" to "x", "sys" to "s"), "n" to 1L))))),
            listOf(mapOf("r" to mapOf("items" to listOf(mapOf("c" to mapOf("code" to "y"))))))
        )

    @Test
    fun `a struct on one page and a string on the other`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("p" to mapOf("x" to 1L))), listOf(mapOf("p" to "flat")))

    @Test
    fun `a list of longs on one page and a mixed list on the other`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("xs" to listOf(1L, 2L))), listOf(mapOf("xs" to listOf("a", mapOf("k" to 1L)))))

    @Test
    fun `struct fields in another order`(al: BufferAllocator) =
        assertBothUnloadAsMerged(
            al,
            listOf(mapOf("s" to linkedMapOf("a" to "A", "b" to "B", "c" to "C"))),
            listOf(mapOf("s" to linkedMapOf("c" to "C", "a" to "A", "b" to "B")))
        )

    @Test
    fun `union legs in another order`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("u" to 1L), mapOf("u" to "x")), listOf(mapOf("u" to "y"), mapOf("u" to 2L)))

    @Test
    fun `timestamp legs in another order`(al: BufferAllocator) {
        val utc = ZonedDateTime.parse("2026-01-01T00:00Z")
        val london = ZonedDateTime.parse("2026-01-01T00:00Z[Europe/London]")
        assertBothUnloadAsMerged(al, listOf(mapOf("t" to utc), mapOf("t" to london)), listOf(mapOf("t" to london), mapOf("t" to utc)))
    }

    @Test
    fun `a nullable single type with nulls, into a union`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("v" to 1L), mapOf("v" to null)), listOf(mapOf("v" to "s")))

    @Test
    fun `an all-null column, into a typed one`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("v" to null)), listOf(mapOf("v" to 1L)))

    @Test
    fun `an empty list leg`(al: BufferAllocator) =
        assertBothUnloadAsMerged(al, listOf(mapOf("m" to listOf(mapOf("c" to "x")))), listOf(mapOf("m" to "flat")))

    @Test
    fun `extension-typed legs`(al: BufferAllocator) =
        assertBothUnloadAsMerged(
            al,
            listOf(mapOf("k" to Keyword.intern("a"), "id" to UUID.fromString("7c3e2bd8-6c9d-4f0b-9d55-3a5a8d7d0e11"))),
            listOf(mapOf("k" to "s", "id" to 1L))
        )

    @Test
    fun `a selection of a page`(al: BufferAllocator) {
        Relation.openFromRows(al, listOf(mapOf("v" to 1L, "s" to mapOf("x" to 1L)), mapOf("v" to 2L, "s" to mapOf("x" to 2L, "y" to "z")))).use { a ->
            Relation.openFromRows(al, listOf(mapOf("v" to "str", "s" to mapOf("y" to "w")))).use { b ->
                val schema = mergedSchema(a, b)
                assertUnloadsAs(al, a.select(intArrayOf(1)), schema)
                assertUnloadsAs(al, a.select(intArrayOf(1, 0)), schema)
            }
        }
    }
}
