package xtdb.pgwire

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

class PgTypesTest {

    private companion object {
        const val INT4_ARRAY_OID = 1007
        const val TEXT_ARRAY_OID = 1009
        const val INT8_ARRAY_OID = 1016
    }

    @Test
    fun `parses an empty array literal`() {
        assertEquals(emptyList<String>(), parsePgArray("{}"))
        assertEquals(emptyList<String>(), parsePgArray(""))
    }

    @Test
    fun `parses unquoted elements`() {
        assertEquals(listOf("a", "b", "c"), parsePgArray("{a,b,c}"))
        assertEquals(listOf("4"), parsePgArray("{4}"))
    }

    @Test
    fun `a comma inside a quoted element stays in that element`() {
        assertEquals(listOf("a,b", "c"), parsePgArray("""{"a,b",c}"""))
        assertEquals(listOf(",", ",,"), parsePgArray("""{",",",,"}"""))
    }

    @Test
    fun `quoted elements are unescaped`() {
        assertEquals(listOf("""say "hi""""), parsePgArray("""{"say \"hi\""}"""))
        assertEquals(listOf("""back\slash"""), parsePgArray("""{"back\\slash"}"""))
        assertEquals(listOf("{brace}"), parsePgArray("""{"{brace}"}"""))
    }

    @Test
    fun `an empty quoted element is an empty string`() {
        assertEquals(listOf("", "a"), parsePgArray("""{"",a}"""))
    }

    @Test
    fun `escapePgArrayElement round-trips through parsePgArray`() {
        val elems = listOf("""a,b""", """c""", """say "hi"""", """back\slash""", "")

        assertEquals(elems, parsePgArray(elems.joinToString(",", "{", "}") { escapePgArrayElement(it) }))
    }

    private fun readText(oid: Int, text: String) = PgType.fromOid(oid)!!.readText(text.toByteArray())

    @Test
    fun `a TEXT array keeps a comma-bearing element whole`() {
        assertEquals(listOf("a,b", "c"), readText(TEXT_ARRAY_OID, """{"a,b",c}"""))
    }

    @Test
    fun `int arrays read from their text literal`() {
        assertEquals(listOf(4L, 6L), readText(INT8_ARRAY_OID, "{4,6}"))
        assertEquals(listOf(4, 6), readText(INT4_ARRAY_OID, "{4,6}"))
    }
}
