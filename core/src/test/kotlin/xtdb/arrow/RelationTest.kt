package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation.Companion.loader
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
                val rel = Relation(listOf(i32, utf8))

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

            Relation(allocator, loader.schema).use { rel ->
                val i32 = rel["i32"]!!
                val utf8 = rel["utf8"]!!

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
            val rel = Relation(listOf(listVec))

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

            Relation(allocator, loader.schema).use { rel ->
                val listVec = rel["list"]!!

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

        val i32Leg = duv.legWriter("i32")
        val utf8Leg = duv.legWriter("utf8")

        i32Leg.writeInt(12)
        utf8Leg.writeObject("hello")
        utf8Leg.writeObject("world!")
        i32Leg.writeInt(34)
        utf8Leg.writeNull()

        val duvValues = listOf(12, "hello", "world!", 34, null)

        assertEquals(duvValues, duv.asList)

        val buf = ByteArrayOutputStream()

        Relation(listOf(duv), duv.valueCount).use { rel ->
            rel.startUnload(Channels.newChannel(buf)).use { unloader ->
                unloader.writePage()
                unloader.end()
            }
        }

        loader(allocator, buf.toByteArray().asChannel).use { loader ->
            loader.loadPage(0, allocator).use { rel ->
                assertEquals(duvValues, rel["duv"]!!.asList)
            }
        }
    }
}