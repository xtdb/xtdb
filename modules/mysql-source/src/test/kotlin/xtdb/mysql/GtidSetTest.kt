package xtdb.mysql

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Unsupported
import java.util.UUID

class GtidSetTest {
    private val a = "9e1c805d-cd8a-11f1-b821-02fc00000001"
    private val b = "0b9a7a52-3c4f-4a5e-9d1c-5a3b6b6a9c11"

    private fun g(id: String, seq: Long) = Gtid(UUID.fromString(id), seq)

    @Test
    fun `parses and prints the server's text form, newlines and all`() {
        val set = GtidSet.parse("$a:1-5:7,\n$b:3")
        assertEquals("$b:3,$a:1-5:7", set.toString())
        assertEquals(set, GtidSet.parse(set.toString()))
    }

    @Test
    fun `an empty string is the empty set`() {
        assertTrue(GtidSet.parse("").isEmpty)
        assertTrue(GtidSet.parse(null).isEmpty)
        assertEquals("", GtidSet.EMPTY.toString())
    }

    @Test
    fun `adding a gtid extends and bridges ranges`() {
        var set = GtidSet.parse("$a:1-3:6-8")
        set += g(a, 4)
        assertEquals("$a:1-4:6-8", set.toString())
        set += g(a, 5)
        assertEquals("$a:1-8", set.toString())
        set += g(a, 10)
        assertEquals("$a:1-8:10", set.toString())
        set += g(b, 1)
        assertEquals("$b:1,$a:1-8:10", set.toString())
    }

    @Test
    fun `counts what the other set is missing`() {
        val executed = GtidSet.parse("$a:1-10,$b:1-3")
        assertEquals(0, executed.countNotIn(executed))
        assertEquals(13, executed.countNotIn(GtidSet.EMPTY))
        assertEquals(5 + 3, executed.countNotIn(GtidSet.parse("$a:1-5")))
        assertEquals(1, executed.countNotIn(GtidSet.parse("$a:1-3:5-10,$b:1-3")))
    }

    @Test
    fun `a tagged GTID set is refused`() {
        assertEquals("xtdb.mysql/tagged-gtid", assertThrows<Unsupported> { GtidSet.parse("$a:1-5,$a:etl:1-3") }.errorCode())
    }

    @Test
    fun `contains`() {
        val set = GtidSet.parse("$a:1-3:6")
        assertTrue(g(a, 2) in set)
        assertTrue(g(a, 6) in set)
        assertFalse(g(a, 4) in set)
        assertFalse(g(b, 2) in set)
    }

    @Test
    fun `subset`() {
        val big = GtidSet.parse("$a:1-10,$b:1-3")
        assertTrue(GtidSet.parse("$a:2-5:7").isSubsetOf(big))
        assertTrue(GtidSet.EMPTY.isSubsetOf(big))
        assertFalse(GtidSet.parse("$a:5-11").isSubsetOf(big))
        assertFalse(GtidSet.parse("$b:4").isSubsetOf(big))
    }
}
