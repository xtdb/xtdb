package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.SeekableByteChannel

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

    internal class ByteBufferChannel(private val buf: ByteBuffer) : SeekableByteChannel {
        override fun read(dst: ByteBuffer): Int {
            val src = buf.slice().limit(dst.remaining())
            dst.put(src)
            val srcPosition = src.position()
            buf.position(buf.position() + srcPosition)
            return srcPosition
        }

        override fun write(src: ByteBuffer) = throw UnsupportedOperationException()

        override fun isOpen() = true
        override fun close() {}
        override fun position() = buf.position().toLong()
        override fun position(newPosition: Long) = apply { buf.position(newPosition.toInt()) }
        override fun size() = buf.limit().toLong()
        override fun truncate(size: Long) = throw UnsupportedOperationException()
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

                        unloader.writeBatch()
                        rel.reset()

                        i32.writeInt(3)
                        utf8.writeObject("world!")
                        rel.endRow()

                        unloader.writeBatch()

                        unloader.endFile()
                    }
            }
        }

        Relation.load(allocator, ByteBufferChannel(ByteBuffer.wrap(buf.toByteArray()))).use { loader ->
            val rel = loader.relation
            val i32 = rel["i32"]!!
            val utf8 = rel["utf8"]!!

            assertEquals(2, loader.batches.size)

            loader.batches[0].load()

            assertEquals(2, rel.rowCount)
            assertEquals(1, i32.getInt(0))
            assertEquals("Hello", utf8.getObject(0))
            assertEquals(2, i32.getInt(1))
            assertTrue(utf8.isNull(1))

            loader.batches[1].load()

            assertEquals(1, rel.rowCount)
            assertEquals(3, i32.getInt(0))
            assertEquals("world!", utf8.getObject(0))

            loader.batches[0].load()

            assertEquals(2, rel.rowCount)
            assertEquals(1, i32.getInt(0))
            assertEquals("Hello", utf8.getObject(0))
            assertEquals(2, i32.getInt(1))
            assertTrue(utf8.isNull(1))
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

                    unloader.writeBatch()
                    rel.reset()

                    listVec.writeObject(list2)
                    rel.endRow()

                    unloader.writeBatch()

                    unloader.endFile()
                }
        }

        Relation.load(allocator, ByteBufferChannel(ByteBuffer.wrap(buf.toByteArray()))).use { loader ->
            val rel = loader.relation
            val listVec = rel["list"]!!

            assertEquals(2, loader.batches.size)

            loader.batches[0].load()

            assertEquals(2, rel.rowCount)
            assertEquals(list0, listVec.getObject(0))
            assertEquals(list1, listVec.getObject(1))

            loader.batches[1].load()

            assertEquals(1, rel.rowCount)
            assertEquals(list2, listVec.getObject(0))

            loader.batches[0].load()

            assertEquals(2, rel.rowCount)
            assertEquals(list0, listVec.getObject(0))
            assertEquals(list1, listVec.getObject(1))
        }
    }

    @Test
    fun testDuvRoundTrip() {
        val duv = DenseUnionVector(
            allocator, "duv", false,
            listOf(
                IntVector(allocator, "i32", false),
                Utf8Vector(allocator, "utf8", true)
            )
        )

        val i32Leg = duv["i32"]!!
        val utf8Leg = duv["utf8"]!!

        i32Leg.writeInt(12)
        utf8Leg.writeObject("hello")
        utf8Leg.writeObject("world!")
        i32Leg.writeInt(34)
        utf8Leg.writeNull()

        val duvValues = listOf(12, "hello", "world!", 34, null)

        assertEquals(duvValues, duv.toList())

        val buf = ByteArrayOutputStream()

        Relation(listOf(duv), duv.valueCount).use { rel ->
            rel.startUnload(Channels.newChannel(buf)).use { unloader ->
                unloader.writeBatch()
                unloader.endFile()
            }
        }

        Relation.load(allocator, ByteBufferChannel(ByteBuffer.wrap(buf.toByteArray()))).use { loader ->
            loader.batches.first().load()

            assertEquals(duvValues, loader.relation["duv"]!!.toList())
        }

    }

}