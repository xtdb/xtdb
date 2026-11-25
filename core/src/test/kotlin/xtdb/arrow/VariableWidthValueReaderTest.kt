package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.test.AllocatorResolver
import java.nio.ByteBuffer

@ExtendWith(AllocatorResolver::class)
class VariableWidthValueReaderTest {
    private val rowCount = 200

    private fun isNullRow(idx: Int) = idx % 7 == 3 || idx == 63 || idx == 64

    private fun value(idx: Int) = "r$idx".repeat(idx % 5)

    private fun VariableWidthVector.fill(withNulls: Boolean) = apply {
        repeat(rowCount) { idx ->
            if (withNulls && isNullRow(idx)) writeNull() else writeBytes(value(idx).toByteArray())
        }
    }

    private val vectors: Map<String, (BufferAllocator, Boolean) -> VariableWidthVector> = mapOf(
        "utf8" to { al, n -> Utf8Vector(al, "v", n) },
        "var-binary" to { al, n -> VarBinaryVector(al, "v", n) },
    )

    private fun outcome(read: () -> Any?): Any? =
        runCatching(read).fold(
            {
                when (it) {
                    is ByteArray -> it.toList()
                    is ByteBuffer -> ByteArray(it.remaining()).also { bytes -> it.duplicate().get(bytes) }.toList()
                    else -> it
                }
            },
            { it::class }
        )

    private fun reads(rdr: ValueReader): List<Any?> = listOf(
        outcome { rdr.isNull },
        outcome { rdr.leg },
        outcome { rdr.readBoolean() },
        outcome { rdr.readByte() },
        outcome { rdr.readShort() },
        outcome { rdr.readInt() },
        outcome { rdr.readLong() },
        outcome { rdr.readFloat() },
        outcome { rdr.readDouble() },
        outcome { rdr.readAsLong() },
        outcome { rdr.readAsDouble() },
        outcome { rdr.readBytes() },
        outcome { rdr.readObject() },
    )

    private fun checkRow(rdr: VectorReader, expected: ValueReader, row: RowValueReader, what: String) {
        expected.pos = row.pos
        assertEquals(reads(expected), reads(row), "$what, row ${row.pos}")
        assertEquals(outcome { rdr.getObject(row.pos, SNAKE_CASE_STRING) }, outcome { row.readObject(SNAKE_CASE_STRING) }, "$what, row ${row.pos}")
    }

    private fun shapes(vec: VectorReader) = mapOf(
        "direct" to vec,
        "select(IntArray)" to vec.select(IntArray(150) { (it * 37) % rowCount }),
        "sorted select(IntArray)" to vec.select(IntArray(100) { it * 2 }),
    )

    @Test
    fun `rows read as the vector's getX and getObject, for every variable-width vector`(al: BufferAllocator) {
        vectors.forEach { (label, open) ->
            listOf(true, false).forEach { withNulls ->
                open(al, withNulls).fill(withNulls).use { vec ->
                    shapes(vec).forEach { (shape, rdr) ->
                        val what = "$label, nulls=$withNulls, $shape"
                        val expected = ValueReader.ForVector(rdr)
                        var count = 0
                        rdr.valueReader().forEach { row ->
                            assertEquals(count++, row.pos, what)
                            checkRow(rdr, expected, row, what)
                        }
                        assertEquals(rdr.valueCount, count, what)
                    }
                }
            }
        }
    }

    @Test
    fun `a row reads bytes even when the rows before it weren't read`(al: BufferAllocator) {
        Utf8Vector(al, "v", true).fill(true).use { vec ->
            val readEvery3rd = vec.valueReader().asSequence()
                .filter { it.pos % 3 == 0 }
                .map { if (it.isNull) null else Charsets.UTF_8.decode(it.readBytes()).toString() }
                .toList()

            assertEquals(List(rowCount) { it }.filter { it % 3 == 0 }.map { if (isNullRow(it)) null else value(it) }, readEvery3rd)
        }
    }

    @Test
    fun `a row reads only bytes and objects, as its vector does`(al: BufferAllocator) {
        Utf8Vector(al, "v", false).fill(false).use { vec ->
            val row = vec.valueReader().next()
            assertEquals("", row.readObject())
            assertThrows<UnsupportedOperationException> { row.readLong() }
            assertThrows<UnsupportedOperationException> { row.readInt() }
        }
    }

    @Test
    fun `seeking across a validity word reads the row sought`(al: BufferAllocator) {
        val seeks = listOf(0, 63, 64, 130, 65, 62, 5, 199, 128, 127, 64, 63, 64, 65)

        vectors.forEach { (label, open) ->
            open(al, true).fill(true).use { vec ->
                mapOf(
                    "direct" to vec,
                    "reversed selection" to vec.select(IntArray(rowCount) { rowCount - 1 - it }),
                ).forEach { (shape, rdr) ->
                    val what = "$label, $shape"
                    val expected = ValueReader.ForVector(rdr)
                    val row = rdr.valueReader()
                    seeks.forEach { idx ->
                        row.pos = idx
                        checkRow(rdr, expected, row, "$what, seek to $idx")
                    }

                    row.pos = 61
                    checkRow(rdr, expected, row.next(), "$what, next after seek")
                    checkRow(rdr, expected, row.next(), "$what, next after seek")
                    checkRow(rdr, expected, row.next(), "$what, next after seek")
                    assertEquals(64, row.pos, what)
                }
            }
        }
    }

    @Test
    fun `a validity buffer shorter than a word reads its nulls`(al: BufferAllocator) {
        val rows = 10
        val values = List(rows) { if (it == 3 || it == 9) null else "v$it" }

        Utf8Vector(al, "v", true).use { vec ->
            al.buffer(2).use { validity ->
                al.buffer((rows + 1) * 4L).use { offsets ->
                    val bytes = values.joinToString("") { it ?: "" }.toByteArray()
                    al.buffer(bytes.size.toLong()).use { data ->
                        validity.setZero(0, validity.capacity())
                        var offset = 0
                        offsets.setInt(0, 0)
                        values.forEachIndexed { idx, v ->
                            if (v != null) validity.setByte(idx / 8L, validity.getByte(idx / 8L).toInt() or (1 shl (idx % 8)))
                            offset += v?.length ?: 0
                            offsets.setInt((idx + 1) * 4L, offset)
                        }
                        data.setBytes(0, bytes)
                        vec.loadPage(mutableListOf(ArrowFieldNode(rows.toLong(), 2)), mutableListOf(validity, offsets, data))
                    }
                }
            }

            assertEquals(2L, vec.validityBuffer!!.buf.capacity())
            assertEquals(values, vec.valueReader().asSequence().map { if (it.isNull) null else it.readObject() }.toList())
        }
    }

    @Test
    fun `a value reader reads its rows while the vector grows under it`(al: BufferAllocator) {
        Utf8Vector(al, "v", true).use { vec ->
            repeat(10) { if (it == 4) vec.writeNull() else vec.writeObject("s$it") }

            fun RowValueReader.bytes() = if (isNull) null else readBytes().let { buf -> ByteArray(buf.remaining()).also { buf.get(it) }.toString(Charsets.UTF_8) }

            val rows = vec.valueReader()
            val seen = mutableListOf<String?>()
            repeat(3) { seen += rows.next().bytes() }

            repeat(100_000) { vec.writeObject("grown") }

            rows.forEach { seen += it.bytes() }
            assertEquals(List(10) { if (it == 4) null else "s$it" }, seen)
            assertFalse(rows.hasNext())
        }
    }

    @Test
    fun `a seek reads the row's offsets afresh, after the vector's page is reloaded`(al: BufferAllocator) {
        fun RowValueReader.string() = Charsets.UTF_8.decode(readBytes()).toString()

        Utf8Vector(al, "v", false).use { vec ->
            listOf("a", "bb", "ccc", "dddd").forEach { vec.writeObject(it) }

            val nextRow = vec.valueReader()
            nextRow.pos = 1
            assertEquals("bb", nextRow.string())

            val sameRow = vec.valueReader()
            sameRow.pos = 2
            assertEquals("ccc", sameRow.string())

            Utf8Vector(al, "src", false).use { src ->
                listOf("wwwww", "xxxxxx", "y", "zz").forEach { src.writeObject(it) }
                val nodes = mutableListOf<ArrowFieldNode>()
                val bufs = mutableListOf<ArrowBuf>()
                src.unloadPage(nodes, bufs)
                vec.loadPage(nodes, bufs)
            }

            nextRow.pos = 2
            assertEquals("y", nextRow.string(), "the row after the last one read")

            sameRow.pos = 2
            assertEquals("y", sameRow.string(), "the row last read")

            assertEquals("zz", nextRow.next().string(), "next() after the seek")
        }
    }
}
