package xtdb

import clojure.lang.Keyword
import clojure.lang.Symbol
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.query.IKeyFn
import xtdb.error.Incorrect
import xtdb.error.Unsupported
import xtdb.http.QueryOptions
import xtdb.http.QueryRequest
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
    fun `parses JSON-LD numbers`() {
        assertEquals(42L, JSON_SERDE.decodeFromString<Any>("{ \"@type\": \"xt:long\", \"@value\": 42 }"))
        assertEquals(42L, JSON_SERDE.decodeFromString<Any>("{ \"@type\": \"xt:long\", \"@value\": \"42\" }"))

        assertEquals(42.0, JSON_SERDE.decodeFromString<Any>("{ \"@type\": \"xt:double\", \"@value\": 42 }"))
        assertEquals(42.0, JSON_SERDE.decodeFromString<Any>("{ \"@type\": \"xt:double\", \"@value\": 42.0 }"))
        assertEquals(42.0, JSON_SERDE.decodeFromString<Any>("{ \"@type\": \"xt:double\", \"@value\": \"42.0\" }"))
    }

    @Test
    fun `round-trips big-decimals`() {
        BigDecimal("1.0010").assertRoundTrip("""{"@type":"xt:decimal","@value":"1.0010"}""")

        "1.0010000000000000000".let {
            BigDecimal(it).assertRoundTrip("""{"@type":"xt:decimal","@value":"$it"}""")
        }

        "1001000000000000000000023421.21923989823429893842".let {
            BigDecimal(it).assertRoundTrip("""{"@type":"xt:decimal","@value":"$it"}""")
        }
    }

    @Test
    fun `date round-tripped to instant`() {
        val instant = Instant.parse("2023-01-01T12:34:56.789Z")
        val date = Date.from(instant)
        val json = JSON_SERDE.encodeToString<Any?>(date)
        assertEquals("""{"@type":"xt:instant","@value":"2023-01-01T12:34:56.789Z"}""", json)
        assertEquals(instant, JSON_SERDE.decodeFromString<Any?>(json))
    }

    @Test
    fun `round-trips java-time`() {
        "2023-01-01T12:34:56.789Z".let { inst ->
            Instant.parse(inst)
                .assertRoundTrip("""{"@type":"xt:instant","@value":"$inst"}""")
        }

        "2023-08-01T12:34:56.789+01:00[Europe/London]".let { zdt ->
            ZonedDateTime.parse(zdt)
                .assertRoundTrip("""{"@type":"xt:timestamptz","@value":"$zdt"}""")
        }

        "PT3H5M12.423S".let { d ->
            Duration.parse(d).assertRoundTrip("""{"@type":"xt:duration","@value":"$d"}""")
        }

        "Europe/London".let { tz ->
            ZoneId.of(tz)
                .assertRoundTrip("""{"@type":"xt:timeZone","@value":"$tz"}""")
        }

        "2023-08-01".let { ld ->
            LocalDate.parse(ld)
                .assertRoundTrip("""{"@type":"xt:date","@value":"$ld"}""")
        }

        "2023-08-01T12:34:56.789".let { ldt ->
            LocalDateTime.parse(ldt)
                .assertRoundTrip("""{"@type":"xt:timestamp","@value":"$ldt"}""")
        }

        "13:31:55".let { lt ->
            LocalTime.parse(lt)
                .assertRoundTrip("""{"@type":"xt:time","@value":"$lt"}""")
        }
    }

    @Test
    fun `round-trips keywords`() {
        Keyword.intern("foo-bar").assertRoundTrip("""{"@type":"xt:keyword","@value":"foo-bar"}""")
        Keyword.intern("xt", "id").assertRoundTrip("""{"@type":"xt:keyword","@value":"xt/id"}""")
    }

    @Test
    fun `round-trips symbols`() {
        Symbol.intern("foo-bar").assertRoundTrip("""{"@type":"xt:symbol","@value":"foo-bar"}""")
        Symbol.intern("xt", "id").assertRoundTrip("""{"@type":"xt:symbol","@value":"xt/id"}""")
    }

    @Test
    fun `round-trips sets`() {
        emptySet<Any>().assertRoundTrip("""{"@type":"xt:set","@value":[]}""")
        setOf(4L, 5L, 6.0).assertRoundTrip("""{"@type":"xt:set","@value":[4,5,6.0]}""")
    }

    @Test
    fun shouldDeserializeIncorrect() {
        Incorrect(
            message = "sort your request out!",
            errorCode = "xtdb/malformed-req",
            data = mapOf("a" to 1L),
        ).assertRoundTrip(
            """{
              "@type": "xt:error",
              "@value": {
                "xtdb.error/message": "sort your request out!",
                "xtdb.error/class": "xtdb.error.Incorrect",
                "xtdb.error/data": {
                  "xtdb.error/code": {"@type":"xt:keyword","@value":"xtdb/malformed-req"},
                  "a": 1
                }
              }
            }""".trimJson()
        )
    }

    @Test
    fun shouldDeserializeUnsupported() {
        Unsupported(
            message = "ruh roh.",
            errorCode = "xtdb/boom",
            mapOf("a" to 1L)
        ).assertRoundTrip(
            """{
              "@type": "xt:error",
              "@value": {
                "xtdb.error/message": "ruh roh.",
                "xtdb.error/class": "xtdb.error.Unsupported",
                "xtdb.error/data": {
                  "xtdb.error/code": {"@type":"xt:keyword","@value":"xtdb/boom"},
                  "a": 1
                }
              }
            }""".trimJson()
        )
    }

    private inline fun <reified T : Any> T.assertRoundTrip2(expectedJson: String) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(expectedJson, actualJson)
        assertEquals(this, JSON_SERDE.decodeFromString<T>(actualJson))
    }

    @Test
    fun shouldDeserializeQueryRequest() {
        QueryRequest(
            "SELECT * FROM foo",
            QueryOptions(
                args = mapOf("foo" to "bar"),
                snapshotTime = Instant.EPOCH + Duration.ofHours(1),
                currentTime = Instant.EPOCH,
                awaitToken = "1",
                txTimeout = Duration.parse("PT3H"),
                defaultTz = ZoneId.of("America/Los_Angeles"),
                explain = true,
                keyFn = IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
            )
        ).assertRoundTrip2(
            """{
                "sql": "SELECT * FROM foo",
                "queryOpts": {"args":{"foo":"bar"},
                              "snapshotTime":"1970-01-01T01:00:00Z",
                              "currentTime": "1970-01-01T00:00:00Z",
                              "awaitToken":"1",
                              "txTimeout":"PT3H",
                              "defaultTz":"America/Los_Angeles",
                              "explain":true,
                              "keyFn":"KEBAB_CASE_KEYWORD"}
              }
            """.trimJson()
        )
    }
}
