package xtdb.util

import clojure.lang.Keyword
import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Incorrect
import java.nio.ByteBuffer
import java.util.HexFormat

class IidTest {

    private val keyValue: Arb<Any> = Arb.choice(
        Arb.long(), Arb.int(), Arb.string(0..20), Arb.uuid(), Arb.string(1..10).map { Keyword.intern(it) }
    )

    private val iid = Arb.byteArray(Arb.constant(16), Arb.byte())

    private fun ByteArray.prefix() = ByteBuffer.wrap(this).getInt(0)

    @Test
    fun `a composite IID's bits under each column's prefix mask are that column's`() = runTest {
        checkAll(1000, Arb.int(2..12).flatMap { n -> Arb.list(iid, n..n) }) { colIids ->
            val n = colIids.size
            val composite = compositeIid(colIids)

            for (c in 0 until n) {
                val mask = prefixMask(n, c)
                assertEquals(colIids[c].prefix() and mask, composite.prefix() and mask, "column $c of $n")
            }
        }
    }

    @Test
    fun `the columns' prefix masks partition the prefix, a nibble at a time`() {
        for (n in 1..12) {
            val masks = (0 until n).map { prefixMask(n, it) }

            assertEquals(-1, masks.fold(0, Int::or), "$n columns cover the prefix")
            assertEquals(32, masks.sumOf(Integer::bitCount), "$n columns' masks are disjoint")
        }

        assertEquals(0xF0F0F0F0.toInt(), prefixMask(2, 0))
        assertEquals(0x0F0F0F0F, prefixMask(2, 1))
        assertEquals(0xF00F00F0.toInt(), prefixMask(3, 0))
        assertEquals(0, prefixMask(9, 8), "a column past the 8th owns no prefix bits")
    }

    @Test
    fun `a single-column key's IID is its value's own IID`() = runTest {
        checkAll(1000, keyValue) { v ->
            assertArrayEquals(v.asIid, keyIid(listOf(v)))
        }
    }

    @Test
    fun `a composite key's IID depends on its columns' order`() = runTest {
        checkAll(1000, keyValue, keyValue) { a, b ->
            if (a != b) assertFalse(keyIid(listOf(a, b)).contentEquals(keyIid(listOf(b, a))))
        }
    }

    @Test
    fun `a composite key's IID is none of its columns' own`() = runTest {
        checkAll(1000, keyValue, keyValue) { a, b ->
            val composite = keyIid(listOf(a, b))
            assertFalse(composite.contentEquals(a.asIid))
            assertFalse(composite.contentEquals(b.asIid))
        }
    }

    @Test
    fun `distinct composite keys have distinct IIDs`() {
        val keys = buildList {
            for (a in 0L until 100L) for (b in 0L until 100L) add(listOf(a, b))
            add(listOf(1L, "1"))
            add(listOf("1", 1L))
            add(listOf(1L, 2L, 3L))
            add(listOf("ab", "c"))
            add(listOf("a", "bc"))
            add(listOf("a", Keyword.intern("a")))
            for (n in 9..10) add((1L..n).toList())
            add((1L..8L).toList() + 10L)
        }

        assertEquals(keys.size, keys.map { HexFormat.of().formatHex(keyIid(it)) }.toSet().size)
    }

    @Test
    fun `a composite key's IID is fixed`() {
        assertEquals("4cd6b26a61efe66be0d435aae0a8209c", HexFormat.of().formatHex(keyIid(listOf(1L, "a"))))
    }

    @Test
    fun `a value that can't key a row can't be part of a key`() {
        assertThrows<Incorrect> { keyIid(listOf(1L, 1.5)) }
        assertThrows<Incorrect> { keyIid(listOf(null, 1L)) }
        assertThrows<Incorrect> { keyIid(listOf(1.5)) }
    }

    @Test
    fun `a key has at least one value`() {
        assertThrows<Incorrect> { keyIid(emptyList<Any>()) }
    }
}
