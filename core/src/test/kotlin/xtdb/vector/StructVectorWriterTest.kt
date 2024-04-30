package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
                dest.maybePromote(srcVec.field)
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
                dest.maybePromote(srcVec.field)
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

    // TODO - test this with non-nullable children - see #3355
    private val intField = Field("int", FieldType.nullable(MinorType.BIGINT.type), emptyList())

    @Test
    fun `test maybePromote on non nullable struct`() {
        val nonNullableStructField = Field("my-struct", FieldType.notNullable(STRUCT_TYPE), listOf(intField))
        val nullableStructField = Field("my-struct", FieldType.nullable(STRUCT_TYPE), listOf(intField))

        nonNullableStructField.createVector(al).use { structVec ->
           val structWriter = writerFor(structVec)
           structWriter.writeObject(mapOf("int" to 42L))

           val newWriter = structWriter.maybePromote(nullableStructField)
           assertEquals(nullableStructField, newWriter.field)

           newWriter.writeNull()
           assertEquals(listOf(mapOf("int" to 42L), null), newWriter.toReader().toList())

           newWriter.close()
        }
    }

    @Test
    fun `test struct-vector child promotion`() {
        val barIntField = Field("bar", FieldType.nullable(MinorType.BIGINT.type), emptyList())
        val struct1Field = Field("struct-int-field", FieldType.nullable(STRUCT_TYPE), listOf(barIntField))
        val barCharField = Field("bar", FieldType.nullable(MinorType.VARCHAR.type), emptyList())
        val struct2Field = Field("struct-char-field", FieldType.nullable(STRUCT_TYPE), listOf(barCharField))

        val struct1Vec = struct1Field.createVector(al)
        val struct2Vec = struct2Field.createVector(al)
        val struct1Writer = writerFor(struct1Vec)
        val struct2Writer = writerFor(struct2Vec)

        struct1Writer.writeObject(mapOf("bar" to 42L))
        struct2Writer.writeObject(mapOf("bar" to "forty-two"))

        StructVector.empty("src", al).use { srcVec ->
            val newStructWriter = writerFor(srcVec).maybePromote(struct1Field).maybePromote(struct2Field)
            val rowCopier1 = newStructWriter.rowCopier(struct1Vec)
            val rowCopier2 = newStructWriter.rowCopier(struct2Vec)

            rowCopier1.copyRow(0)
            rowCopier2.copyRow(0)

            assertEquals(listOf(mapOf("bar" to 42L), mapOf("bar" to "forty-two")), newStructWriter.toReader().toList())

            newStructWriter.close()
        }

        struct1Writer.close()
        struct2Writer.close()
    }
}
