package xtdb.vector

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class IVectorWriterTest {

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
    fun `monomorphic promoteChildren test`() {
        val nullField = Field("null", FieldType.nullable(ArrowType.Null.INSTANCE), emptyList())
        val intFieldNotNull = Field("i32", FieldType.notNullable(Types.MinorType.INT.type), emptyList())
        val intField= Field("i32", FieldType.nullable(Types.MinorType.INT.type), emptyList())
        val doubleField = Field("f64", FieldType.notNullable(Types.MinorType.FLOAT8.type), emptyList())
        val duvSingleLegged = Field( "int-duv", UNION_FIELD_TYPE, listOf(intFieldNotNull))
        val duvDoubleLeggedWithNull = Field( "int-duv-nullable", UNION_FIELD_TYPE, listOf(intFieldNotNull, nullField))
        val duvDoubleLegged = Field( "duv-doubled-legged", UNION_FIELD_TYPE, listOf(intFieldNotNull, doubleField))
        val duvOnlyNullLeg = Field("duv-only-null-leg", UNION_FIELD_TYPE, listOf(nullField))

        writerFor(nullField.createVector(al)).use { w ->
            w.promoteChildren(nullField)
            assertThrows<FieldMismatch> { w.promoteChildren(duvSingleLegged) }
            assertThrows<FieldMismatch> { w.promoteChildren(duvDoubleLeggedWithNull) }
            assertThrows<FieldMismatch> { w.promoteChildren(duvDoubleLegged) }
            w.promoteChildren(duvOnlyNullLeg)
        }

        writerFor(intField.createVector(al)).use { w ->
            w.promoteChildren(nullField)
            w.promoteChildren(duvSingleLegged)
            w.promoteChildren(duvDoubleLeggedWithNull)
            assertThrows<FieldMismatch> { w.promoteChildren(duvDoubleLegged) }
            w.promoteChildren(duvOnlyNullLeg)
        }
    }
}