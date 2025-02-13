package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE

class StructVectorWriterTest {

    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    @Test
    fun `test write some structs with writeObject`() {
        val objs = listOf(
            mapOf("a" to 12, "b" to 24.0),
            mapOf("a" to 20, "b" to 3.4),
        )

        writerFor(StructVector.empty("foo", al)).use { w ->
            objs.forEach { w.writeObject(it) }
            assertEquals(objs, w.toReader().toList())
        }
    }

    @Test
    fun `test copy some structs`() {
        val objs = listOf(
            mapOf("a" to 12, "b" to 24.0),
            mapOf("a" to 20, "b" to 3.4),
        )

        StructVector.empty("src", al).use { srcVec ->
            writerFor(srcVec).apply {
                objs.forEach { writeObject(it) }
            }

            writerFor(StructVector.empty("dest", al)).use { dest ->
                dest.rowCopier(srcVec).apply {
                    copyRow(0); copyRow(1)
                }
                assertEquals(objs, dest.toReader().toList())
            }
        }
    }

    @Test
    fun `test copy a DUV`() {
        val objs = listOf(
            mapOf("a" to 12.0),
            mapOf("a" to 20),
        )

        StructVector.empty("src", al).use { srcVec ->
            val w = writerFor(srcVec).apply {
                objs.forEach { writeObject(it) }
            }

            assertEquals(objs, w.toReader().toList())

            writerFor(StructVector.empty("dest", al)).use { dest ->
                dest.rowCopier(srcVec).apply {
                    copyRow(0); copyRow(1)
                }
                assertEquals(objs, dest.toReader().toList())
            }
        }
    }

    @Test
    fun `test promote`() {
        val objs = listOf(
            mapOf("a" to 12, "b" to 24.0),
            mapOf("a" to 20),
            mapOf("b" to 12),
        )

        writerFor(StructVector.empty("foo", al)).use { w ->
            objs.forEach { w.writeObject(it) }
            assertEquals(objs, w.toReader().toList())
        }
    }

    @Test
    fun `test StructVector handles DUV children`() {
        val child = Field("child", UNION_FIELD_TYPE, emptyList())
        val structField = Field("foo", FieldType.notNullable(STRUCT_TYPE), listOf(child))

        StructVector(structField, al, null).use { structVec ->
            assertEquals(structField, structVec.field)
        }
    }

    @Test
    fun `test nullable struct-vector key doesn't promote for non-nullable source`() {
        val aField = Field("a", FieldType.nullable(MinorType.BIGINT.type), emptyList())
        val structField = Field("src", FieldType.nullable(STRUCT_TYPE), listOf(aField))

        StructVector.empty("src", al).use { srcVec ->
            val structWriter = writerFor(srcVec)

            val nullWriter = structWriter.structKeyWriter("a", FieldType.nullable(MinorType.BIGINT.type))

            assertEquals(structField, structWriter.field)
            assertEquals(aField, nullWriter.field)

            val nnWriter = structWriter.structKeyWriter("a", FieldType.notNullable(MinorType.BIGINT.type))

            assertEquals(structField, structWriter.field)
            assertEquals(aField, nnWriter.field)
        }
    }

    @Test
    fun `promoting from non nullable struct to nullable struct throws FieldMismatch`() {
        val fooField = Field("foo", FieldType.notNullable(MinorType.BIGINT.type), emptyList())
        val nullableStructField = Field("src", FieldType.nullable(STRUCT_TYPE), listOf(fooField))
        val nonNullableStructField = Field("src", FieldType.notNullable(STRUCT_TYPE), listOf(fooField))
        nonNullableStructField.createVector(al).use { srcVec ->
            val structWriter = writerFor(srcVec)

            assertThrows<FieldMismatch>(
                { structWriter.promoteChildren(nullableStructField) },
            )
        }
    }
}
