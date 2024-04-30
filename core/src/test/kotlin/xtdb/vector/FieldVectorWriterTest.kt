package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.withName

class FieldVectorWriterTest {

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
    fun `test maybePromote (nullable to nonNullable) on scalar vectors`() {
        val intField = Field("my-initial-int", FieldType.notNullable(Types.MinorType.BIGINT.type), emptyList())
        val intVec = intField.createVector(al)
        val intWriter = writerFor(intVec)

        intWriter.writeObject(42L)

        val newWriter = intWriter.maybePromote(Field("my-initial-int-nullable", FieldType.nullable(Types.MinorType.BIGINT.type), emptyList()))

        assertEquals(Field("my-initial-int", FieldType.nullable(Types.MinorType.BIGINT.type), emptyList()), newWriter.field)

        newWriter.close()
    }

    @Test
    fun `test maybePromote (i64 to DUV(i64, utf8) on scalar vectors`() {
        val intField = Field("my-initial-int", FieldType.nullable(Types.MinorType.BIGINT.type), emptyList())
        val strField = Field("utf8", FieldType.nullable(Types.MinorType.VARCHAR.type), emptyList())
        val intVec = intField.createVector(al)
        val intWriter = writerFor(intVec)

        intWriter.writeObject(42L)

        val newWriter = intWriter.maybePromote(strField)

        assertEquals(Field("my-initial-int", FieldType.notNullable(Types.MinorType.DENSEUNION.type), listOf(intField.withName("i64"), strField)), newWriter.field)

        newWriter.close()
    }

    @Test
    fun `test maybePromote and rowCopier on ScalarVectors`() {
        val intField = Field("int", FieldType.nullable(Types.MinorType.BIGINT.type), emptyList())
        val strField = Field("str", FieldType.nullable(Types.MinorType.VARCHAR.type), emptyList())
        val intVec = intField.createVector(al)
        val strVec = strField.createVector(al)
        val intWriter = writerFor(intVec)
        val strWriter = writerFor(strVec)

        intWriter.writeObject(42L)
        strWriter.writeObject("forty-two")

        val dest = writerFor(intField.createVector(al)).maybePromote(intField).maybePromote(strField)
        dest.rowCopier(intVec).copyRow(0)
        dest.rowCopier(strVec).copyRow(0)

        assertEquals(listOf(42L, "forty-two"), dest.toReader().toList())

        intWriter.close()
        strWriter.close()
        dest.close()
    }
}