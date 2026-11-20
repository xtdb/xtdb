package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.test.AllocatorResolver
import xtdb.util.Hasher

private const val ROWS = 150

@ExtendWith(AllocatorResolver::class)
class MaskedVectorTest {

    private class Spec(val open: (BufferAllocator) -> VectorReader, val readable: (Int) -> Boolean = { true })

    private val vectors: Map<String, Spec> = mapOf(
        "nullable i64" to Spec({ al -> Vector.fromList(al, "col", List(ROWS) { if (it % 7 == 3) null else it * 10L }) }),
        "nullable utf8" to Spec({ al -> Vector.fromList(al, "col", List(ROWS) { if (it % 5 == 1) null else "s$it" }) }),
        "struct" to Spec({ al ->
            Vector.fromList(al, "col", List(ROWS) { mapOf("fooBar" to it.toLong(), "baz" to if (it % 3 == 0) null else "s$it") })
        }),
        "list" to Spec({ al -> Vector.fromList(al, "col", List(ROWS) { if (it % 6 == 0) null else listOf(it, it + 1) }) }),
        "union with absent rows" to Spec(
            { al ->
                DenseUnionVector(al, "col").apply {
                    repeat(ROWS) {
                        when (it % 4) {
                            0 -> writeObject(it.toLong())
                            1 -> writeObject("s$it")
                            2 -> writeObject(null)
                            else -> writeUndefined()
                        }
                    }
                }
            },
            readable = { it % 4 != 3 }),
    )

    private val masks: Map<String, (Int) -> Boolean> = mapOf(
        "either side of each word boundary" to { it in setOf(0, 62, 63, 64, 65, 127, 128, 149) },
        "every third row" to { it % 3 == 0 },
        "all but the first and last" to { it != 0 && it != ROWS - 1 },
        "the last row only" to { it == ROWS - 1 },
        "all" to { true },
        "none" to { false },
    )

    private fun VectorReader.byIdx(f: VectorReader.(Int) -> Any?) = List(valueCount) { f(it) }

    private fun RowValueReader.rows() = asSequence().map { Triple(it.pos, it.isNull, it.readObject(SNAKE_CASE_STRING)) }.toList()

    private fun ValueReader.read(): Any? = when {
        isNull -> null
        else -> runCatching { readList().let { l -> List(l.size()) { l.nth(it).read() } } }
            .recoverCatching { readStruct().mapValues { (_, v) -> v.read() } }
            .getOrElse { readObject() }
    }

    private fun ValueReader.at(idx: Int) = run { pos = idx; Pair(isNull, read()) }

    private fun BufferAllocator.hashes(vec: VectorReader, sel: IntArray?) =
        IntVector.open(this, "h", false).use { vec.hashTo(it, Hasher.Xx(), sel); it.asList }

    private fun BufferAllocator.copied(vec: VectorReader) = vec.openDirectSlice(this).use { it.asList }

    private fun checkAgrees(al: BufferAllocator, masked: VectorReader, expected: VectorReader, what: String) {
        assertEquals(expected.valueCount, masked.valueCount, "$what, valueCount")
        assertEquals(expected.asList, masked.asList, "$what, asList")
        assertEquals(expected.valueReader().rows(), masked.valueReader().rows(), "$what, valueReader walked")
        assertEquals(expected.byIdx { getObject(it) }, masked.byIdx { getObject(it) }, "$what, getObject")
        assertEquals(expected.byIdx { isNull(it) }, masked.byIdx { isNull(it) }, "$what, isNull")
        assertEquals(expected.byIdx { hashCode(it, Hasher.Xx()) }, masked.byIdx { hashCode(it, Hasher.Xx()) }, "$what, hashCode")

        val rdr = masked.valueReader()
        val expectedRdr = expected.valueReader()
        assertEquals(expected.byIdx { expectedRdr.at(it) }, masked.byIdx { rdr.at(it) }, "$what, valueReader seeked")

        assertEquals(al.hashes(expected, null), al.hashes(masked, null), "$what, hashTo")
        assertEquals(al.copied(expected), al.copied(masked), "$what, rowCopier")

        val n = masked.valueCount
        val sel = intArrayOf(n - 1, 0, n / 2, n / 2).filter { it in 0..<n }.toIntArray()
        assertEquals(al.hashes(expected, sel), al.hashes(masked, sel), "$what, hashTo over a selection")
        assertEquals(expected.select(sel).asList, masked.select(sel).asList, "$what, select(IntArray)")
        assertEquals(expected.select(n / 3, n / 3).asList, masked.select(n / 3, n / 3).asList, "$what, select(start, len)")
    }

    @Test
    fun `reads agree with the selection of the mask's rows`(al: BufferAllocator) {
        vectors.forEach { (vecLabel, spec) ->
            spec.open(al).use { vec ->
                masks.forEach { (maskLabel, set) ->
                    val mask = rowMask(ROWS) { set(it) && spec.readable(it) }
                    val what = "$vecLabel, $maskLabel"
                    checkAgrees(al, MaskedVector(vec, mask), vec.select(mask.toSelection()), what)

                    MaskedVector(vec, mask).openSlice(al).use { masked ->
                        checkAgrees(al, masked, vec.select(mask.toSelection()), "$what, openSlice")
                    }
                }
            }
        }
    }

    @Test
    fun `a struct's children are masked by the same rows`(al: BufferAllocator) {
        vectors.getValue("struct").open(al).use { vec ->
            val mask = rowMask(ROWS) { it % 3 != 1 }
            val masked = MaskedVector(vec, mask)
            val expected = vec.select(mask.toSelection())

            for (key in listOf("fooBar", "baz"))
                checkAgrees(al, masked[key], expected[key], key)
        }
    }

    @Test
    fun `a masked vector's value reader seeks to a logical row and walks on from it`(al: BufferAllocator) {
        vectors.getValue("nullable i64").open(al).use { vec ->
            val mask = rowMask(ROWS) { it % 3 == 0 || it in 60..70 }
            val masked = MaskedVector(vec, mask)
            val expected = masked.asList

            val rows = masked.valueReader()
            for (seekTo in listOf(30, 0, 21, 22, masked.valueCount - 1, 5)) {
                rows.pos = seekTo
                assertEquals(seekTo, rows.pos)
                assertEquals(expected[seekTo], rows.readObject(SNAKE_CASE_STRING), "seek to $seekTo")

                val walked = mutableListOf<Any?>()
                while (rows.hasNext()) walked += rows.next().readObject(SNAKE_CASE_STRING)
                assertEquals(expected.drop(seekTo + 1), walked, "walk on from $seekTo")
            }
        }
    }

    @Test
    fun `a mask over a masked vector reads its own rows of the inner mask's`(al: BufferAllocator) {
        vectors.getValue("nullable i64").open(al).use { vec ->
            val outer = rowMask(ROWS) { it % 2 == 0 }
            val inner = MaskedVector(vec, outer)
            val mask = rowMask(inner.valueCount) { it % 3 != 0 }

            checkAgrees(al, MaskedVector(inner, mask), vec.select(outer.toSelection()).select(mask.toSelection()), "nested")
            checkAgrees(al, MaskedVector(inner, mask), MaskedVector(inner, mask).select(0, mask.cardinality), "nested, slice")
        }
    }

    @Test
    fun `the value reader is exhausted where the mask's rows run out`(al: BufferAllocator) {
        vectors.getValue("nullable i64").open(al).use { vec ->
            val rows = MaskedVector(vec, rowMask(ROWS) { it == 64 }).valueReader()
            assertSame(rows, rows.next())
            assertEquals(640L, rows.readLong())
            assertFalse(rows.hasNext())
            assertThrows<NoSuchElementException> { rows.next() }
        }
    }

    @Test
    fun `an empty mask over an empty vector walks nothing`(al: BufferAllocator) {
        Vector.fromList(al, "col", emptyList<Long>()).use { vec ->
            val masked = MaskedVector(vec, rowMask(0) { true })
            assertEquals(0, masked.valueCount)
            assertFalse(masked.valueReader().hasNext())
        }
    }

    @Test
    fun `closing a masked vector closes its inner vector once`(al: BufferAllocator) {
        val vec = Vector.fromList(al, "col", List(ROWS) { it.toLong() })
        val masked = MaskedVector(vec, rowMask(ROWS) { it % 2 == 0 })
        masked.getLong(3)
        masked.close()
    }
}
