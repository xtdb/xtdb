package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.isSubType
import xtdb.toLeg
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE

class DenseUnionVectorWriterTest {
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
    fun `test rowCopier for DUV with nullable field, #3258`() {
        UNION_FIELD_TYPE.createNewSingleVector("target", al, null).use { destVec ->
            var destWriter = writerFor(destVec)

            // first we add the not-nullable field
            UNION_FIELD_TYPE.createNewSingleVector("src", al, null).use { srcVec ->
                val srcWriter = writerFor(srcVec)
                val f64Writer = srcWriter.legWriter(Keyword.intern("f64"), FieldType.notNullable(MinorType.FLOAT8.type))
                f64Writer.writeDouble(12.3)

                destWriter = destWriter.maybePromote(srcWriter.field)

                destWriter.rowCopier(srcVec).copyRow(0)
            }

            // then we simulate a nullable field coming in
            UNION_FIELD_TYPE.createNewSingleVector("src", al, null).use { srcVec ->
                val srcWriter = writerFor(srcVec)
                val f64Writer = srcWriter.legWriter(Keyword.intern("f64"), FieldType.nullable(MinorType.FLOAT8.type))
                f64Writer.writeNull()
                f64Writer.writeDouble(18.9)

                destWriter = destWriter.maybePromote(srcWriter.field)

                destWriter.rowCopier(srcVec).apply { copyRow(0); copyRow(1) }
            }

            destWriter.syncValueCount()

            assertEquals(
                Field(
                    "target", UNION_FIELD_TYPE,
                    listOf(
                        Field("f64", FieldType.nullable(MinorType.FLOAT8.type), emptyList()),
                    )
                ),
                destVec.field
            )

            assertEquals(listOf(12.3, null, 18.9), destWriter.toReader().toList())
        }
    }

    @Test
    fun `test maybePromote for DUV with scalar vectors`(){
        UNION_FIELD_TYPE.createNewSingleVector("duv", al, null).use {  vec ->
            var writer = writerFor(vec)

            val intField = Field("i64", FieldType.nullable(MinorType.BIGINT.type), emptyList())

            writer = writer.maybePromote(intField)
            assertEquals(Field("duv", UNION_FIELD_TYPE, listOf(intField)), writer.field)

            val strField = Field("utf8", FieldType.nullable(MinorType.VARCHAR.type), emptyList())
            writer = writer.maybePromote(strField)
            assertEquals(Field("duv", UNION_FIELD_TYPE, listOf(intField, strField)), writer.field)
        }
    }

    @Test
    fun `test row copiers to DUV`(){
        UNION_FIELD_TYPE.createNewSingleVector("duv", al, null).use {  vec ->
            val writer = writerFor(vec)

            val intWriter = writerFor(Field("i64", FieldType.nullable(MinorType.BIGINT.type), emptyList()).createVector(al))
            val strWriter = writerFor(Field("utf8", FieldType.nullable(MinorType.VARCHAR.type), emptyList()).createVector(al))

            intWriter.writeLong(42L)
            strWriter.writeObject("forty-two")

            val dest = writer.maybePromote(intWriter.field).maybePromote(strWriter.field)
            dest.rowCopier(intWriter.vector).apply {
                copyRow(0)
            }

            dest.rowCopier(strWriter.vector).apply {
                copyRow(0)
            }

            assertEquals(listOf(42L, "forty-two"), dest.toReader().toList())

            intWriter.close()
            strWriter.close()
            dest.close()
        }
    }

    @Test
    fun `test row copiers with DUV + complex types`(){
        UNION_FIELD_TYPE.createNewSingleVector("duv", al, null).use {  vec ->
            val writer = writerFor(vec)

            val structField = Field("struct", FieldType.nullable(STRUCT_TYPE), emptyList())
            val structVec = structField.createVector(al)
            val structWriter = writerFor(structVec)

            structWriter.writeObject(mapOf("a" to 12, "b" to 24.0))
            structWriter.writeObject(mapOf("a" to 20))
            structWriter.writeObject(mapOf("b" to 12))

            val intField = Field("i64", FieldType.nullable(MinorType.BIGINT.type), emptyList())
            val complexWriter1 = structWriter.maybePromote(intField)
            complexWriter1.legWriter(intField.fieldType.type.toLeg()).writeLong(42L)

            val strField = Field("utf8", FieldType.nullable(MinorType.VARCHAR.type), emptyList())
            val strVec = strField.createVector(al)
            val strWriter = writerFor(strVec)

            strWriter.writeObject("forty-two")
            val complexWriter2 = strWriter.maybePromote(structField)
            complexWriter2.legWriter(structField.fieldType.type.toLeg()).writeObject(mapOf("a" to "forty-three", "b" to 42L))


            val dest = writer.maybePromote(complexWriter1.field).maybePromote(complexWriter2.field)
            val rowCopier1 = dest.rowCopier(complexWriter1.vector)
            val rowCopier2 = dest.rowCopier(complexWriter2.vector)
            rowCopier1.copyRow(0)
            rowCopier2.copyRow(0)
            rowCopier1.copyRow(3)
            rowCopier2.copyRow(1)

            assertEquals(
                listOf(
                    mapOf("a" to 12, "b" to 24.0),
                    "forty-two",
                    42L,
                    mapOf("a" to "forty-three", "b" to 42L)),
                dest.toReader().toList()
            )

            complexWriter1.close()
            complexWriter2.close()
            dest.close()
        }
    }

    private val NULL_FIELD = Field("\$data\$", FieldType.nullable(MinorType.NULL.type), null)

    @Test
    fun `test null promotion`() {
        UNION_FIELD_TYPE.createNewSingleVector("duv", al, null).use {  vec ->
            val writer = writerFor(vec).maybePromote(NULL_FIELD)

            assertEquals(isSubType(NULL_FIELD, writer.field), true)
        }
    }

    @Test
    fun `test promotion, leg updating and closing of vector`() {
        UNION_FIELD_TYPE.createNewSingleVector("duv", al, null).use {  vec ->
            val writer = writerFor(vec)

            val intField = Field("i64", FieldType.nullable(MinorType.BIGINT.type), emptyList())
            val strField = Field("utf8", FieldType.nullable(MinorType.VARCHAR.type), emptyList())
            val promoted = writer.maybePromote(intField).maybePromote(strField)

            promoted.writeObject(42L)
            promoted.writeObject("forty-two")

            assertEquals(listOf(42L, "forty-two", ), promoted.toReader().toList())

            // TODO Why doesn't this throw?
            // vec.close()
        }
    }
}
