package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.vector.extensions.KeywordType
import org.apache.arrow.vector.types.pojo.ArrowType.List.INSTANCE as LIST_TYPE

private val NN_KEYWORD = FieldType.notNullable(KeywordType)
private val NN_UTF8 = FieldType.notNullable(Types.MinorType.VARCHAR.type)
private val NN_STRUCT = FieldType.notNullable(Types.MinorType.STRUCT.type)

private val PROMOTES_STRUCTS_FIELD =
    Field(
        "my-list", FieldType.notNullable(LIST_TYPE),
        listOf(
            Field(
                "\$data\$", UNION_FIELD_TYPE,
                listOf(
                    Field(
                        "struct", NN_STRUCT,
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
    fun `promotes structs`() {
        writerFor(ListVector("my-list", al, FieldType.notNullable(LIST_TYPE), null)).use { listWtr ->
            val obj = listOf(mapOf("a" to Keyword.intern("foo")), mapOf("a" to "foo"))

            listWtr.writeObject(obj)

            assertEquals(listOf(obj), listWtr.toReader().toList())
            assertEquals(PROMOTES_STRUCTS_FIELD, listWtr.field)
        }
    }
}
