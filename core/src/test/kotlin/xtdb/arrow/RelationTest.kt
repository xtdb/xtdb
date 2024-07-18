package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
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

    private class ByteBufferChannel(private val buf: ByteBuffer) : SeekableByteChannel {
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
    fun testRoundTrip() {
        val buf = ByteArrayOutputStream()

        IntVector(Field.nonNullableI32("a"), allocator).use { a ->
            val rel = Relation(listOf(a))

            rel.startUnload(Channels.newChannel(buf))
                .use { unloader ->
                    a.writeInt(1)
                    rel.endRow()
                    a.writeInt(2)
                    rel.endRow()

                    unloader.writeBatch()
                    rel.reset()

                    a.writeInt(3)
                    rel.endRow()

                    unloader.writeBatch()

                    unloader.endFile()
                }
        }

        Relation.load(allocator, ByteBufferChannel(ByteBuffer.wrap(buf.toByteArray()))).use { loader ->
            val rel = loader.relation
            val a = rel["a"]!!

            assertEquals(2, loader.batches.size)

            loader.batches[0].load()

            assertEquals(2, rel.rowCount)
            assertEquals(1, a.getInt(0))
            assertEquals(2, a.getInt(1))

            loader.batches[1].load()

            assertEquals(1, rel.rowCount)
            assertEquals(3, a.getInt(0))

            loader.batches[0].load()

            assertEquals(2, rel.rowCount)
            assertEquals(1, a.getInt(0))
            assertEquals(2, a.getInt(1))
        }
    }

}