package xtdb

import clojure.lang.Keyword
import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.error.Incorrect
import xtdb.error.Unsupported
import java.math.BigDecimal
import java.time.*
import java.util.*

internal fun String.trimJson() =
    trimIndent()
        .replace(": ", ":")
        .replace(", ", ",")
        .replace(Regex("\n\\s*"), "")

class JsonSerdeTest {

    private fun Any?.assertRoundTrip(expectedJson: String) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(expectedJson, actualJson)
        assertEquals(this, JSON_SERDE.decodeFromString(actualJson))
    }

    private fun Any?.assertRoundTrip(expectedJson: String, expectedThis: Any) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(expectedJson, actualJson)
        assertEquals(expectedThis, JSON_SERDE.decodeFromString(actualJson))
    }

    @Test
    fun `roundtrips JSON literals`() {
        null.assertRoundTrip("null")

        listOf(42L, "bell", true, mapOf("a" to 12.5, "b" to "bar"))
            .assertRoundTrip("""[42,"bell",true,{"a":12.5,"b":"bar"}]""")
        mapOf(Keyword.intern("c") to "foo")
            .assertRoundTrip("""{"c":"foo"}""", mapOf("c" to "foo"))
    }

    @Test
    fun `encodes big-decimals as strings`() {
        val json = JSON_SERDE.encodeToString<Any>(BigDecimal("1.0010"))
        assertEquals("\"1.0010\"", json)

        assertEquals("\"1.0010000000000000000\"", JSON_SERDE.encodeToString<Any>(BigDecimal("1.0010000000000000000")))
        assertEquals("\"1001000000000000000000023421.21923989823429893842\"",
            JSON_SERDE.encodeToString<Any>(BigDecimal("1001000000000000000000023421.21923989823429893842")))
    }

    @Test
    fun `encodes date as instant string`() {
        val instant = Instant.parse("2023-01-01T12:34:56.789Z")
        val date = Date.from(instant)
        val json = JSON_SERDE.encodeToString<Any?>(date)
        assertEquals("\"2023-01-01T12:34:56.789Z\"", json)
    }

    @Test
    fun `encodes java-time as strings`() {
        assertEquals("\"2023-01-01T12:34:56.789Z\"",
            JSON_SERDE.encodeToString<Any>(Instant.parse("2023-01-01T12:34:56.789Z")))

        assertEquals("\"2023-08-01T12:34:56.789+01:00[Europe/London]\"",
            JSON_SERDE.encodeToString<Any>(ZonedDateTime.parse("2023-08-01T12:34:56.789+01:00[Europe/London]")))

        assertEquals("\"PT3H5M12.423S\"",
            JSON_SERDE.encodeToString<Any>(Duration.parse("PT3H5M12.423S")))

        assertEquals("\"Europe/London\"",
            JSON_SERDE.encodeToString<Any>(ZoneId.of("Europe/London")))

        assertEquals("\"2023-08-01\"",
            JSON_SERDE.encodeToString<Any>(LocalDate.parse("2023-08-01")))

        assertEquals("\"2023-08-01T12:34:56.789\"",
            JSON_SERDE.encodeToString<Any>(LocalDateTime.parse("2023-08-01T12:34:56.789")))

        assertEquals("\"13:31:55\"",
            JSON_SERDE.encodeToString<Any>(LocalTime.parse("13:31:55")))
    }

    @Test
    fun `encodes keywords as strings`() {
        assertEquals("\"foo-bar\"", JSON_SERDE.encodeToString<Any>(Keyword.intern("foo-bar")))
        assertEquals("\"xt/id\"", JSON_SERDE.encodeToString<Any>(Keyword.intern("xt", "id")))
    }

    @Test
    fun `encodes symbols as strings`() {
        assertEquals("\"foo-bar\"", JSON_SERDE.encodeToString<Any>(Symbol.intern("foo-bar")))
        assertEquals("\"xt/id\"", JSON_SERDE.encodeToString<Any>(Symbol.intern("xt", "id")))
    }

    @Test
    fun `encodes sets as arrays`() {
        assertEquals("[]", JSON_SERDE.encodeToString<Any>(emptySet<Any>()))
        assertEquals("[4,5,6.0]", JSON_SERDE.encodeToString<Any>(setOf(4L, 5L, 6.0)))
    }

    @Test
    fun shouldSerializeIncorrect() {
        val json = JSON_SERDE.encodeToString<Any>(Incorrect(
            message = "sort your request out!",
            errorCode = "xtdb/malformed-req",
            data = mapOf("a" to 1L),
        ))
        assertEquals(
            """{"@type":"xt:incorrect","@value":{"xtdb.error/message":"sort your request out!","xtdb.error/data":{"xtdb.error/code":{"@type":"xt:keyword","@value":"xtdb/malformed-req"},"a":1}}}""",
            json
        )
    }

    @Test
    fun shouldSerializeUnsupported() {
        val json = JSON_SERDE.encodeToString<Any>(Unsupported(
            message = "ruh roh.",
            errorCode = "xtdb/boom",
            mapOf("a" to 1L)
        ))
        assertEquals(
            """{"@type":"xt:unsupported","@value":{"xtdb.error/message":"ruh roh.","xtdb.error/data":{"xtdb.error/code":{"@type":"xt:keyword","@value":"xtdb/boom"},"a":1}}}""",
            json
        )
    }

    private inline fun <reified T : Any> T.assertRoundTrip2(expectedJson: String) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(expectedJson, actualJson)
        assertEquals(this, JSON_SERDE.decodeFromString<T>(actualJson))
    }
}
