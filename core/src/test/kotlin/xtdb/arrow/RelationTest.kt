package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation.Companion.loader
import xtdb.asKeyword
import xtdb.types.Fields
import xtdb.types.Schema
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

class RelationTest {

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
    fun testScalarRoundTrip() {
        val buf = ByteArrayOutputStream()

        IntVector(allocator, "i32", false).use { i32 ->
            Utf8Vector(allocator, "utf8", true).use { utf8 ->
                val rel = Relation(allocator, listOf(i32, utf8), 0)

                rel.startUnload(Channels.newChannel(buf))
                    .use { unloader ->
                        i32.writeInt(1)
                        utf8.writeObject("Hello")
                        rel.endRow()
                        i32.writeInt(2)
                        utf8.writeNull()
                        rel.endRow()

                        unloader.writePage()
                        rel.clear()

                        i32.writeInt(3)
                        utf8.writeObject("world!")
                        rel.endRow()

                        unloader.writePage()

                        unloader.end()
                    }
            }
        }

        loader(allocator, buf.toByteArray().asChannel).use { loader ->
            assertEquals(2, loader.pageCount)

            Relation.open(allocator, loader.schema).use { rel ->
                val i32 = rel["i32"]
                val utf8 = rel["utf8"]

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(1, i32.getInt(0))
                assertEquals("Hello", utf8.getObject(0))
                assertEquals(2, i32.getInt(1))
                assertTrue(utf8.isNull(1))

                loader.loadPage(1, rel)

                assertEquals(1, rel.rowCount)
                assertEquals(3, i32.getInt(0))
                assertEquals("world!", utf8.getObject(0))

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(1, i32.getInt(0))
                assertEquals("Hello", utf8.getObject(0))
                assertEquals(2, i32.getInt(1))
                assertTrue(utf8.isNull(1))
            }
        }
    }

    @Test
    fun testListRoundTrip() {
        val buf = ByteArrayOutputStream()

        val elVector = IntVector(allocator, "els", true)

        val list0 = listOf(1, 4, null, 12)
        val list1 = listOf(8)
        val list2 = listOf(1, 0, -1, null)

        ListVector(allocator, "list", false, elVector).use { listVec ->
            val rel = Relation(allocator, listOf(listVec), 0)

            rel.startUnload(Channels.newChannel(buf))
                .use { unloader ->
                    listVec.writeObject(list0)
                    rel.endRow()
                    listVec.writeObject(list1)
                    rel.endRow()

                    unloader.writePage()
                    rel.clear()

                    listVec.writeObject(list2)
                    rel.endRow()

                    unloader.writePage()

                    unloader.end()
                }
        }

        loader(allocator, buf.toByteArray().asChannel).use { loader ->
            assertEquals(2, loader.pageCount)

            Relation.open(allocator, loader.schema).use { rel ->
                val listVec = rel["list"]

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(list0, listVec.getObject(0))
                assertEquals(list1, listVec.getObject(1))

                loader.loadPage(1, rel)

                assertEquals(1, rel.rowCount)
                assertEquals(list2, listVec.getObject(0))

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(list0, listVec.getObject(0))
                assertEquals(list1, listVec.getObject(1))
            }
        }
    }

    @Test
    fun testDuvRoundTrip() {
        val duv = DenseUnionVector(
            allocator, "duv",
            listOf(
                IntVector(allocator, "i32", false),
                Utf8Vector(allocator, "utf8", true)
            )
        )

        val i32Leg = duv.vectorFor("i32")
        val utf8Leg = duv.vectorFor("utf8")

        i32Leg.writeInt(12)
        utf8Leg.writeObject("hello")
        utf8Leg.writeObject("world!")
        i32Leg.writeInt(34)
        utf8Leg.writeNull()

        val duvValues = listOf(12, "hello", "world!", 34, null)

        assertEquals(duvValues, duv.toList())

        val buf = ByteArrayOutputStream()

        Relation(allocator, listOf(duv), duv.valueCount).use { rel ->
            rel.startUnload(Channels.newChannel(buf)).use { unloader ->
                unloader.writePage()
                unloader.end()
            }
        }

        loader(allocator, buf.toByteArray().asChannel).use { loader ->
            loader.loadPage(0, allocator).use { rel ->
                assertEquals(duvValues, rel["duv"].toList())
            }
        }
    }

    @Test
    fun testStreamRoundTrip() {
        val row1 = mapOf("i32" to 4)
        val row2 = mapOf("i32" to 8)

        val bytes = Relation.open(allocator, linkedMapOf("i32" to Fields.I32)).use { rel ->
            rel.writeRows(row1, row2)
            rel.asArrowStream
        }

        val rows = Relation.StreamLoader(allocator, bytes.asChannel).use { loader ->
            Relation.open(allocator, loader.schema).use { rel ->
                assertTrue(loader.loadNextPage(rel))

                rel.toMaps(SNAKE_CASE_STRING)
                    .also {
                        assertFalse(loader.loadNextPage(rel))
                    }
            }
        }

        assertEquals(listOf(row1, row2), rows)
    }

    @Test
    fun testPromotion() {
        Relation(allocator).use { rel ->
            val intVec = rel.vectorFor("foo", Fields.I32.fieldType)
            intVec.writeInt(32)
            rel.endRow()

            assertEquals(listOf(mapOf("foo".asKeyword to 32)), rel.toMaps())

            val strVec = rel.vectorFor("bar", Fields.UTF8.fieldType)
            strVec.writeObject("hello")
            rel.endRow()

            assertEquals(
                Schema("foo" to Fields.I32.nullable, "bar" to Fields.UTF8.nullable),
                rel.schema
            )

            assertEquals(
                listOf(
                    mapOf("foo".asKeyword to 32),
                    mapOf("bar".asKeyword to "hello")
                ),
                rel.toMaps()
            )
        }
    }

    @Test
    fun testWriteRows() {
        val rows = listOf(
            mapOf("foo" to 32),
            mapOf("foo" to 64, "bar" to "hello"),
            mapOf("bar" to listOf("a", "promotion"))
        )
        Relation.openFromRows(allocator, rows).use { rel ->
            assertEquals(
                Schema(
                    "foo" to Fields.I32.nullable,
                    "bar" to Fields.Union(
                        "utf8" to Fields.UTF8.nullable,
                        "list" to Fields.List(Fields.UTF8, "\$data$")
                    )
                ),
                rel.schema
            )

            assertEquals(rows, rel.toMaps(SNAKE_CASE_STRING))
        }
    }
}