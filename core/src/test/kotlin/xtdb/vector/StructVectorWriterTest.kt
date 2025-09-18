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
            assertEquals(objs, w.asReader.toList())
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
                assertEquals(objs, dest.asReader.toList())
            }
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
}
