package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

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
            val destWriter = writerFor(destVec)

            // first we add the not-nullable field
            UNION_FIELD_TYPE.createNewSingleVector("src", al, null).use { srcVec ->
                val srcWriter = writerFor(srcVec)
                val f64Writer = srcWriter.legWriter("f64", FieldType.notNullable(MinorType.FLOAT8.type))
                f64Writer.writeDouble(12.3)

                destWriter.rowCopier(srcVec).copyRow(0)
            }

            // then we simulate a nullable field coming in
            UNION_FIELD_TYPE.createNewSingleVector("src", al, null).use { srcVec ->
                val srcWriter = writerFor(srcVec)
                val f64Writer = srcWriter.legWriter("f64", FieldType.nullable(MinorType.FLOAT8.type))
                f64Writer.writeNull()
                f64Writer.writeDouble(18.9)

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
}
