package xtdb

import clojure.lang.Keyword
import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
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
    fun `decodes an integer beyond int64 exactly`() {
        assertEquals(9223372036854775807L, decode("9223372036854775807"))

        assertEquals(BigDecimal("9223372036854775808"), decode("9223372036854775808"))
        assertEquals(BigDecimal("18446744073709551615"), decode("18446744073709551615"))
        assertEquals(BigDecimal("18446744073709551623"), decode("18446744073709551623"))
        assertEquals(BigDecimal("-18446744073709551623"), decode("-18446744073709551623"))

        assertEquals(mapOf("n" to BigDecimal("18446744073709551623")), decode("""{"n": 18446744073709551623}"""))
    }

    @Test
    fun `an integer too wide for a decimal keeps its double approximation`() {
        assertEquals(BigDecimal("9".repeat(64)), decode("9".repeat(64)))

        assertEquals(1.0E64, decode("1" + "0".repeat(64)))
    }

    @Test
    fun `a non-integral number stays a double`() {
        assertEquals(1.5, decode("1.5"))
        assertEquals(1.0E30, decode("1e30"))
    }

    @Test
    fun `encodes date as instant string`() {
        val instant = Instant.parse("2023-01-01T12:34:56.789Z")
        val date = Date.from(instant)
        val json = JSON_SERDE.encodeToString<Any?>(date)
        assertEquals("\"2023-01-01T12:34:56.789Z\"", json)
    }

    @Test
    fun `encodes ZonedDateTime as OffsetDateTime string`() {
        // ZonedDateTime.toString() produces "[zone]" suffix which breaks JS Date parsing, so we encode as OffsetDateTime
        assertEquals("\"2023-08-01T12:34:56.789+01:00\"",
            JSON_SERDE.encodeToString<Any>(ZonedDateTime.parse("2023-08-01T12:34:56.789+01:00[Europe/London]")))

        assertEquals("\"2026-01-01T10:00:00.001Z\"",
            JSON_SERDE.encodeToString<Any>(ZonedDateTime.parse("2026-01-01T10:00:00.001Z[UTC]")))
    }

    @Test
    fun `encodes java-time as strings`() {
        assertEquals("\"2023-01-01T12:34:56.789Z\"",
            JSON_SERDE.encodeToString<Any>(Instant.parse("2023-01-01T12:34:56.789Z")))

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
            """{"category":"incorrect","code":"xtdb/malformed-req","message":"sort your request out!","data":{"a":1}}""",
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
            """{"category":"unsupported","code":"xtdb/boom","message":"ruh roh.","data":{"a":1}}""",
            json
        )
    }

    private inline fun <reified T : Any> T.assertRoundTrip2(expectedJson: String) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(expectedJson, actualJson)
        assertEquals(this, JSON_SERDE.decodeFromString<T>(actualJson))
    }
}
