package xtdb.pgwire

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull

class PgTypesTest {

    private companion object {
        const val JSON_OID = 114
        const val INT4_ARRAY_OID = 1007
        const val TEXT_ARRAY_OID = 1009
        const val INT8_ARRAY_OID = 1016
        const val JSONB_OID = 3802
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
    fun `a bare NULL is a null element`() {
        assertEquals(listOf("4", null, "6"), parsePgArray("{4,NULL,6}"))
        assertEquals(listOf(null), parsePgArray("{NULL}"))
        assertEquals(listOf(null, null), parsePgArray("{NULL,null}"))
    }

    @Test
    fun `a quoted or escaped NULL is the string`() {
        assertEquals(listOf("NULL"), parsePgArray("""{"NULL"}"""))
        assertEquals(listOf("NULL"), parsePgArray("""{\NULL}"""))
    }

    @Test
    fun `a quoted NULL beside a bare one keeps its quoting to itself`() {
        assertEquals(listOf("x", null, "NULL"), parsePgArray("""{x,NULL,"NULL"}"""))
        assertEquals(listOf("NULL", null), parsePgArray("""{"NULL",NULL}"""))
        assertEquals(listOf("hello world", "plain"), parsePgArray("""{"hello world",plain}"""))
    }

    @Test
    fun `unquoted elements are trimmed`() {
        assertEquals(listOf("4", null, "6"), parsePgArray("{ 4 , NULL , 6 }"))
        assertEquals(listOf(" a "), parsePgArray("""{" a "}"""))
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

    @Test
    fun `a null element reads as null rather than throwing`() {
        assertEquals(listOf(4L, null, 6L), readText(INT8_ARRAY_OID, "{4,NULL,6}"))
        assertEquals(listOf(4, null, 6), readText(INT4_ARRAY_OID, "{4,NULL,6}"))
        assertEquals(listOf("a", null), readText(TEXT_ARRAY_OID, "{a,NULL}"))
    }

    @Test
    fun `a top-level JSON null reads as null`() {
        assertNull(readText(JSONB_OID, "null"))
        assertNull(readText(JSON_OID, "null"))
    }

    @Test
    fun `a JSON null under a key reads as a null value under that key`() {
        assertEquals(mapOf("a" to null, "b" to 1L), readText(JSONB_OID, """{"a": null, "b": 1}"""))
        assertEquals(listOf(1L, null), readText(JSONB_OID, "[1, null]"))
    }
}
