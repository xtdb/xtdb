package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation.Companion.loader
import xtdb.toFieldType
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

class StructVectorTest {
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun structVectorValues() {
        val children = linkedMapOf(
            "i32" to IntVector(allocator, "i32", false),
            "utf8" to Utf8Vector(allocator, "utf8", true)
        )

        val m1 = mapOf("i32" to 4, "utf8" to "Hello")
        val m2 = mapOf("i32" to 8)
        val m3 = mapOf("i32" to 12, "utf8" to "world!")

        StructVector(allocator, "struct", true, children).use { structVec ->
            structVec.writeObject(m1)
            structVec.writeObject(m2)
            structVec.writeObject(m3)

            assertEquals(listOf(m1, m2, m3), structVec.asList)
        }
    }

    @Test
    fun useNestedWriters() {
        val children = linkedMapOf(
            "i32" to IntVector(allocator, "i32", false),
            "utf8" to Utf8Vector(allocator, "utf8", true)
        )

        StructVector(allocator, "struct", false, children).use { structVec ->
            val i32Writer = structVec.keyWriter("i32")
            val utf8Writer = structVec.keyWriter("utf8")

            i32Writer.writeInt(4)
            utf8Writer.writeObject("Hello")
            structVec.endStruct()

            i32Writer.writeInt(8)
            structVec.endStruct()

            i32Writer.writeInt(12)
            utf8Writer.writeObject("world!")
            structVec.endStruct()

            assertEquals(
                listOf(
                    mapOf("i32" to 4, "utf8" to "Hello"),
                    mapOf("i32" to 8),
                    mapOf("i32" to 12, "utf8" to "world!")
                ),
                structVec.asList
            )
        }
    }

    @Test
    fun createsMissingVectors() {
        StructVector(allocator, "struct", false).use { structVec ->
            val i32Writer = structVec.keyWriter("i32", 4.toFieldType())

            i32Writer.writeInt(4)
            structVec.endStruct()

            val utf8Writer = structVec.keyWriter("utf8", "foo".toFieldType())

            i32Writer.writeInt(8)
            utf8Writer.writeObject("Hello")
            structVec.endStruct()

            i32Writer.writeInt(12)
            utf8Writer.writeObject("world!")
            structVec.endStruct()

            assertEquals(
                listOf(
                    mapOf("i32" to 4),
                    mapOf("i32" to 8, "utf8" to "Hello"),
                    mapOf("i32" to 12, "utf8" to "world!")
                ),
                structVec.asList
            )
        }

    }

    @Test
    fun roundTripsThroughFile() {
        val m1 = mapOf("i32" to 4, "utf8" to "Hello")
        val m2 = mapOf("i32" to 8)
        val m3 = mapOf("i32" to 12, "utf8" to "world!")

        val buf = ByteArrayOutputStream()

        val children = linkedMapOf(
            "i32" to IntVector(allocator, "i32", false),
            "utf8" to Utf8Vector(allocator, "utf8", true)
        )

        StructVector(allocator, "struct", false, children).use { structVec ->
            val rel = Relation(listOf(structVec))

            rel.startUnload(Channels.newChannel(buf))
                .use { unloader ->
                    structVec.writeObject(m1)
                    rel.endRow()
                    structVec.writeObject(m2)
                    rel.endRow()

                    unloader.writePage()
                    rel.clear()

                    structVec.writeObject(m3)
                    rel.endRow()

                    unloader.writePage()

                    unloader.end()
                }
        }

        loader(allocator, buf.toByteArray().asChannel).use { loader ->
            Relation(allocator, loader.schema).use { rel ->
                val structVec = rel["struct"]!!

                assertEquals(2, loader.pageCount)

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(m1, structVec.getObject(0))
                assertEquals(m2, structVec.getObject(1))

                loader.loadPage(1, rel)

                assertEquals(1, rel.rowCount)
                assertEquals(m3, structVec.getObject(0))

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(m1, structVec.getObject(0))
                assertEquals(m2, structVec.getObject(1))
            }
        }
    }

    @Test
    fun `from null into struct vector`() {
        NullVector("v1" ).use { nullVector ->
            nullVector.writeNull()
            nullVector.writeNull()
            nullVector.writeNull()

            StructVector(allocator, "v2", true,
                linkedMapOf(
                    "i32" to IntVector(allocator, "i32", false),
                    "utf8" to Utf8Vector(allocator, "utf8", true)
                )).use { copy ->
                val copier = nullVector.rowCopier(copy)
                copier.copyRow(0)
                copier.copyRow(1)
                copier.copyRow(2)

                assertEquals(3, copy.valueCount)
                assertNull(copy.getObject(1))
            }
        }
    }

    @Test
    fun `use nested writers with DUV and nulls - 3946`() {
        val children = linkedMapOf(
            "i32" to IntVector(allocator, "i32", false),
            "duv" to DenseUnionVector(allocator, "duv", listOf(
                IntVector(allocator, "i32", false),
                Utf8Vector(allocator, "utf8", true)
            )))

        StructVector(allocator, "struct", false, children).use { structVec ->
            val i32Writer = structVec.keyWriter("i32")
            val duvWriter = structVec.keyWriter("duv")
            val nestedUtf8Writer = duvWriter.legWriter("utf8")
            i32Writer.writeInt(1)
            nestedUtf8Writer.writeObject("one")
            structVec.endStruct()

            i32Writer.writeInt(2)
            structVec.endStruct()

            assertEquals(
                listOf(
                    mapOf("i32" to 1, "duv" to "one"),
                    mapOf("i32" to 2),
                ),
                structVec.asList
            )
        }
    }

    @Test
    fun `can add keys to a struct`() {
        StructVector(allocator, "struct", false).use { structVec ->
            val els = listOf(
                mapOf("i32" to 4),
                mapOf("i32" to 8, "utf8" to "Hello"),
                mapOf("utf8" to "World")
            )
            structVec.writeObject(els[0])
            structVec.writeObject(els[1])
            structVec.writeObject(els[2])

            assertEquals(els, structVec.asList)

            assertEquals(
                Field("struct", FieldType(false, STRUCT_TYPE, null), listOf(
                    Field("i32", FieldType(true, I32_TYPE, null), emptyList()),
                    Field("utf8", FieldType(true, UTF8_TYPE, null), emptyList())
                )),
                structVec.field
            )
        }
    }

    @Test
    fun `writing null to struct doesn't make the underlying vectors nullable`() {
        StructVector(allocator, "struct", false).use { structVec ->
            structVec.writeObject(mapOf("i32" to 4, "utf8" to "Hello"))
            structVec.writeNull()

            assertEquals(
                listOf(mapOf("i32" to 4, "utf8" to "Hello"), null),
                structVec.asList
            )

            assertEquals(
                Field("struct", FieldType(true, STRUCT_TYPE, null), listOf(
                    Field("i32", FieldType(false, I32_TYPE, null), emptyList()),
                    Field("utf8", FieldType(false, UTF8_TYPE, null), emptyList())
                )),
                structVec.field
            )
        }
    }

    @Test
    fun `promoting a struct key to a union`() {
        StructVector(allocator, "struct", false).use { structVec ->
            val els = listOf(mapOf("a" to 4), mapOf("a" to "Hello"))
                .onEach(structVec::writeObject)

            assertEquals(els, structVec.asList)

            assertEquals(
                Field("struct", FieldType(false, STRUCT_TYPE, null), listOf(
                    Field("a", FieldType(false, UNION_TYPE, null), listOf(
                        Field("i32", FieldType(false, I32_TYPE, null), emptyList()),
                        Field("utf8", FieldType(false, UTF8_TYPE, null), emptyList())
                    ))
                )),
                structVec.field
            )
        }
    }
}
