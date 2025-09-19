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
import xtdb.types.Arrow.withName
import xtdb.types.Type
import xtdb.types.Type.Companion.asListOf
import xtdb.types.Type.Companion.unionOf
import xtdb.vector.extensions.KeywordType
import org.apache.arrow.vector.types.pojo.ArrowType.List.INSTANCE as LIST_TYPE

private val NN_KEYWORD = FieldType.notNullable(KeywordType)
private val NN_UTF8 = FieldType.notNullable(Types.MinorType.VARCHAR.type)
private val NN_I32 = FieldType.notNullable(Types.MinorType.INT.type)
private val NULL_I32 = FieldType.nullable(Types.MinorType.INT.type)
private val NN_STRUCT = FieldType.notNullable(Types.MinorType.STRUCT.type)

private val PROMOTES_STRUCTS_FIELD =
    Field(
        "my-list", FieldType.notNullable(LIST_TYPE),
        listOf(
            Field(
                "\$data\$", NN_STRUCT,
                listOf(
                    Field(
                        "a", UNION_FIELD_TYPE,
                        listOf(
                            Field("keyword", NN_KEYWORD, emptyList()),
                            Field("utf8", NN_UTF8, emptyList())
                        )
                    )
                )
            )
        )
    )

class ListVectorWriterTest {

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
    fun `test write some lists with writeObject`() {
        val lists = listOf(listOf(12, 20), listOf(24, 32))

        writerFor("foo".asListOf(Type.I32).createVector(al)).use { w ->
            lists.forEach { w.writeObject(it) }
            assertEquals(lists, w.asReader.asList)
        }
    }

    @Test
    fun `test copy some lists`() {
        val lists = listOf(listOf(12, 20), listOf(24, 32))

        "src".asListOf(Type.I32).createVector(al).use { srcVec ->
            writerFor(srcVec).apply {
                lists.forEach { writeObject(it) }
            }

            writerFor(srcVec.field.withName("dest").createVector(al)).use { dest ->
                dest.rowCopier(srcVec).apply {
                    copyRow(0); copyRow(1)
                }
                assertEquals(lists, dest.asReader.asList)
                assertEquals("dest".asListOf(Type.I32), dest.field)
            }
        }
    }

    @Test
    fun `test copy a DUV`() {
        val lists = listOf(
            listOf(12, 24.0),
            listOf(18.4, 36),
        )

        "src".asListOf(unionOf(Type.I32.asLegField, Type.F32.asLegField)).createVector(al).use { srcVec ->
            val w = writerFor(srcVec).apply {
                lists.forEach { writeObject(it) }
            }

            assertEquals(lists, w.asReader.asList)

            writerFor(srcVec.field.withName("dest").createVector(al)).use { dest ->
                dest.rowCopier(srcVec).apply {
                    copyRow(0); copyRow(1)
                }
                assertEquals(lists, dest.asReader.asList)
            }
        }
    }
}
