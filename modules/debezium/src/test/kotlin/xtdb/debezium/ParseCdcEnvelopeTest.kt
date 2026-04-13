package xtdb.debezium

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertThrows
import xtdb.error.Incorrect

class ParseCdcEnvelopeTest {

    @Test
    fun `bare payload without schema envelope`() {
        val json = """{"op":"c","after":{"_id":1,"name":"Alice"},"source":{"schema":"public","table":"users"}}"""

        val out = parseCdcEnvelope(json)

        assertEquals("c", out["op"])
        @Suppress("UNCHECKED_CAST")
        val source = out["source"] as Map<String, Any?>
        assertEquals("public", source["schema"])
        assertEquals("users", source["table"])
    }

    @Test
    fun `schema-payload envelope is unwrapped`() {
        val json = """
            {
              "schema": {"type":"struct", "fields": []},
              "payload": {"op":"c","after":{"_id":1},"source":{"schema":"s","table":"t"}}
            }
        """.trimIndent()

        val out = parseCdcEnvelope(json)

        assertEquals("c", out["op"])
    }

    @Test
    fun `nested arrays and objects convert to jvm values`() {
        val json = """{"op":"u","arr":[1,2,3],"nested":{"a":true,"b":null}}"""

        val out = parseCdcEnvelope(json)

        @Suppress("UNCHECKED_CAST")
        val arr = out["arr"] as List<Any?>
        assertEquals(listOf(1L, 2L, 3L), arr)

        @Suppress("UNCHECKED_CAST")
        val nested = out["nested"] as Map<String, Any?>
        assertEquals(true, nested["a"])
        assertNull(nested["b"])
    }

    @Test
    fun `number types preserve long and double correctly`() {
        val json = """{"intVal": 42, "longVal": 9999999999, "doubleVal": 3.14}"""

        val out = parseCdcEnvelope(json)

        assertEquals(42L, out["intVal"])
        assertEquals(9999999999L, out["longVal"])
        assertEquals(3.14, out["doubleVal"])
    }

    @Test
    fun `invalid json rejected with Incorrect error`() {
        val bad = """{"op":"c","malformed"""

        val err = assertThrows<Incorrect> { parseCdcEnvelope(bad) }
        assertTrue(err.message!!.contains("Invalid CDC message"))
    }

    @Test
    fun `non-object payload is rejected`() {
        val bad = """{"schema": {}, "payload": ["not","an","object"]}"""

        val err = assertThrows<Incorrect> { parseCdcEnvelope(bad) }
        assertTrue(err.message!!.contains("'payload' was not a JSON object"))
    }
}
