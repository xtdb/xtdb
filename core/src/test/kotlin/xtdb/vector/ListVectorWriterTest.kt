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
import xtdb.withName
import org.apache.arrow.vector.types.pojo.ArrowType.List.INSTANCE as LIST_TYPE

private val NN_KEYWORD = FieldType.notNullable(KeywordType)
private val NN_UTF8 = FieldType.notNullable(Types.MinorType.VARCHAR.type)
private val NN_I32 = FieldType.notNullable(Types.MinorType.INT.type)
private val NULL_I32 = FieldType.nullable(Types.MinorType.INT.type)
private val NN_STRUCT = FieldType.notNullable(Types.MinorType.STRUCT.type)
private val NN_STR = FieldType.notNullable(Types.MinorType.VARCHAR.type)

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
                dest.maybePromote(srcVec.field)
                dest.rowCopier(srcVec).apply {
                    copyRow(0); copyRow(1)
                }
                assertEquals(lists, dest.toReader().toList())
                assertEquals(
                    Field(
                        "dest", FieldType.nullable(LIST_TYPE),
                        listOf(Field("\$data\$", NN_I32, emptyList()))
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
                dest.maybePromote(srcVec.field)
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

    private val intField = Field("int", NN_I32, emptyList())
    private val strField = Field("str", NN_STR, emptyList())


    @Test
    fun `test maybePromote on non nullable list`() {
        val nonNullableListField = Field("my-int-list", FieldType.notNullable(LIST_TYPE), listOf(intField))
        val nullableListField = Field("my-int-list", FieldType.nullable(LIST_TYPE), listOf(Field("\$data\$", NN_I32, emptyList())))

        nonNullableListField.createVector(al).use { listVec ->
            val listWrt = writerFor(listVec)
            listWrt.writeObject(listOf(42, 24))

            val newWriter = listWrt.maybePromote(nullableListField)
            assertEquals(nullableListField, newWriter.field)

            newWriter.writeNull()
            assertEquals(listOf(listOf(42, 24), null), newWriter.toReader().toList())

            newWriter.close()
        }
    }

    @Test
    fun `test maybePromote (i64 to DUV(i64, utf8) on list elements`() {
        val intListField = Field("my-list", FieldType.notNullable(LIST_TYPE), listOf(intField))
        val strListField = Field("my-str-list", FieldType.notNullable(LIST_TYPE), listOf(strField))

        intListField.createVector(al).use { listVec ->
            val listWrt = writerFor(listVec)
            listWrt.writeObject(listOf(42, 24))

            val newWriter = listWrt.maybePromote(strListField)
            assertEquals(
                Field("my-list", FieldType.notNullable(LIST_TYPE),
                    listOf(Field("\$data\$", FieldType.notNullable(Types.MinorType.DENSEUNION.type),
                        listOf(intField.withName("i32"), strField.withName("utf8"))))),
                newWriter.field)

            newWriter.writeObject(listOf("foo", "bar"))

            assertEquals(listOf(listOf(42, 24), listOf("foo", "bar")), newWriter.toReader().toList())

            newWriter.close()
        }
    }

    @Test
    fun `test maybePromote and rowCopier on list`() {
        val listWithIntField = Field("my-int-list", FieldType.notNullable(LIST_TYPE), listOf(Field("int", NN_I32, emptyList())))
        val listWithCharField = Field("my-str-list", FieldType.notNullable(LIST_TYPE), listOf(Field("str", NN_STR, emptyList())))

        val intListVec = listWithIntField.createVector(al).also { listVec ->
            val listWrt = writerFor(listVec)
            listWrt.writeObject(listOf(42, 24))
        }

        val strListVec = listWithIntField.createVector(al).also { listVec ->
            val listWrt = writerFor(listVec)
            listWrt.writeObject(listOf("foo", "bar"))
        }

        val destWriter = writerFor(ListVector.empty("dest", al)).maybePromote(listWithIntField).maybePromote(listWithCharField)
        destWriter.rowCopier(intListVec).copyRow(0)
        destWriter.rowCopier(strListVec).copyRow(0)

        assertEquals(listOf(listOf(42, 24), listOf("foo", "bar")), destWriter.toReader().toList())

        intListVec.close()
        strListVec.close()
        destWriter.close()
    }
}
