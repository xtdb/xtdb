package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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

        writerFor(ListVector.empty("foo", al)).use { w ->
            lists.forEach { w.writeObject(it) }
            assertEquals(lists, w.toReader().toList())
        }
    }

    @Test
    fun `test copy some lists`() {
        val lists = listOf(listOf(12, 20), listOf(24, 32))

        ListVector.empty("src", al).use { srcVec ->
            writerFor(srcVec).apply {
                lists.forEach { writeObject(it) }
            }

            writerFor(ListVector.empty("dest", al)).use { dest ->
                dest.rowCopier(srcVec).apply {
                    copyRow(0); copyRow(1)
                }
                assertEquals(lists, dest.toReader().toList())
                assertEquals(
                    Field(
                        "dest", FieldType.nullable(LIST_TYPE),
                        listOf(
                            Field(
                                "\$data\$", UNION_FIELD_TYPE,
                                listOf(Field("i32", NN_I32, emptyList()))
                            )
                        )
                    ),
                    dest.field
                )
            }
        }
    }

    @Test
    fun `test copy a DUV`() {
        val lists = listOf(
            listOf(12, 24.0),
            listOf(18.4, 36),
        )

        ListVector.empty("src", al).use { srcVec ->
            val w = writerFor(srcVec).apply {
                lists.forEach { writeObject(it) }
            }

            assertEquals(lists, w.toReader().toList())

            writerFor(ListVector.empty("dest", al)).use { dest ->
                dest.rowCopier(srcVec).apply {
                    copyRow(0); copyRow(1)
                }
                assertEquals(lists, dest.toReader().toList())
            }
        }
    }

    @Test
    fun `test promote`() {
        val lists = listOf(
            listOf(12, 24),
            listOf(null, 36),
            listOf("foo"),
        )

        writerFor(ListVector.empty("foo", al)).use { w ->
            w.writeObject(lists[0])

            assertEquals(
                Field(
                    "foo", FieldType.nullable(LIST_TYPE),
                    listOf(Field("\$data\$", NN_I32, emptyList()))
                ), w.field
            )

            w.writeObject(lists[1])

            assertEquals(
                Field(
                    "foo", FieldType.nullable(LIST_TYPE),
                    listOf(Field("\$data\$", NULL_I32, emptyList()))
                ), w.field
            )

            w.writeObject(lists[2])

            assertEquals(
                Field(
                    "foo", FieldType.nullable(LIST_TYPE),
                    listOf(
                        Field(
                            "\$data\$", UNION_FIELD_TYPE,
                            listOf(
                                Field("i32", NULL_I32, emptyList()),
                                Field("utf8", NN_UTF8, emptyList()),
                            )
                        )
                    )
                ), w.field
            )

            assertEquals(lists, w.toReader().toList())
        }
    }

    @Test
    fun `promotes structs`() {
        writerFor(ListVector("my-list", al, FieldType.notNullable(LIST_TYPE), null)).use { listWtr ->
            val obj = listOf(mapOf("a" to Keyword.intern("foo")), mapOf("a" to "foo"))

            listWtr.writeObject(obj)

            assertEquals(listOf(obj), listWtr.toReader().toList())
            assertEquals(PROMOTES_STRUCTS_FIELD, listWtr.field)
        }
    }
}
