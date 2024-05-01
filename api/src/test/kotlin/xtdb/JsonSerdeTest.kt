package xtdb

import clojure.lang.Keyword
import clojure.lang.Symbol
import kotlinx.serialization.encodeToString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.query.*
import xtdb.api.query.Expr.Bool.FALSE
import xtdb.api.query.Expr.Bool.TRUE
import xtdb.api.query.Expr.Null
import xtdb.api.query.Exprs.lVar
import xtdb.api.query.Exprs.list
import xtdb.api.query.Exprs.map
import xtdb.api.query.Exprs.param
import xtdb.api.query.Exprs.q
import xtdb.api.query.Exprs.set
import xtdb.api.query.Exprs.`val`
import xtdb.api.query.Queries.aggregate
import xtdb.api.query.Queries.from
import xtdb.api.query.Queries.join
import xtdb.api.query.Queries.leftJoin
import xtdb.api.query.Queries.limit
import xtdb.api.query.Queries.offset
import xtdb.api.query.Queries.orderBy
import xtdb.api.query.Queries.orderSpec
import xtdb.api.query.Queries.pipeline
import xtdb.api.query.Queries.relation
import xtdb.api.query.Queries.returning
import xtdb.api.query.Queries.unify
import xtdb.api.query.Queries.unnest
import xtdb.api.query.Queries.where
import xtdb.api.query.Queries.with
import xtdb.api.query.Queries.without
import xtdb.api.tx.TxOp
import xtdb.api.tx.TxOps.insert
import xtdb.api.tx.TxOps.putDocs
import xtdb.api.tx.TxOps.update
import xtdb.api.txKey
import java.time.*
import java.util.*

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

    private fun String.trimJson(): String {
        return this.trimIndent()
            .replace(": ", ":")
            .replace(", ", ",")
            .replace(Regex("\n\\s+"), "")
    }

    @Test
    fun shouldDeserializeIllegalArgException() {
        IllegalArgumentException(
            Keyword.intern("xtdb", "malformed-req"),
            "sort your request out!",
            mapOf(Keyword.intern("a") to 1L),
        ).assertRoundTrip(
            """{
              "@type": "xt:error",
              "@value": {
                "xtdb.error/message": "sort your request out!",
                "xtdb.error/class": "xtdb.IllegalArgumentException",
                "xtdb.error/error-key": "xtdb/malformed-req",
                "xtdb.error/data": {
                  "a": 1
                }
              }
            }""".trimJson()
        )
    }


    @Test
    fun shouldDeserializeRuntimeException() {
        RuntimeException(
            Keyword.intern("xtdb", "boom"),
            "ruh roh.",
            mapOf(Keyword.intern("a") to 1L)
        ).assertRoundTrip(
            """{
              "@type": "xt:error",
              "@value": {
                "xtdb.error/message": "ruh roh.",
                "xtdb.error/class": "xtdb.RuntimeException",
                "xtdb.error/error-key": "xtdb/boom",
                "xtdb.error/data": {
                  "a": 1
                }
              }
            }""".trimJson()
        )
    }

    private fun TxOp.assertRoundTripTxOp(expectedJson: String) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(expectedJson, actualJson)
        assertEquals(this, JSON_SERDE.decodeFromString(TxOp.Serde, actualJson))
    }

    @Test
    fun shouldDeserializeTxOp() {
        putDocs("foo", mapOf("xt/id" to "foo", "bar" to Instant.parse("2023-01-01T12:34:56.789Z")))
            .assertRoundTripTxOp(
                """{
                  "into": "foo",
                  "putDocs": [{
                    "xt/id": "foo",
                    "bar": {
                      "@type": "xt:instant",
                      "@value": "2023-01-01T12:34:56.789Z"
                    }
                  }]
                }
                """.trimJson()
            )
        putDocs("foo", mapOf("xt/id" to "foo", "bar" to Instant.parse("2023-01-01T12:34:56.789Z")))
            .startingFrom(Instant.EPOCH)
            .until(Instant.parse("2023-01-01T12:34:56.789Z"))
            .assertRoundTripTxOp(
                """{
                  "into": "foo",
                  "putDocs": [{
                    "xt/id": "foo",
                      "bar": {
                        "@type": "xt:instant",
                        "@value": "2023-01-01T12:34:56.789Z"
                      }
                  }],
                 "validFrom": "1970-01-01T00:00:00Z",
                 "validTo": "2023-01-01T12:34:56.789Z"
                }
                """.trimJson()
            )
        TxOp.XtqlAndArgs(insert("foo", from("docs").bind("xt/id").build()))
            .assertRoundTripTxOp(
                """{
                  "insertInto": "foo",
                  "query": {
                    "from": "docs",
                    "bind": [{"xt/id":{"xt:lvar":"xt/id"}}]
                  },
                  "argRows": null
                }
                """.trimJson()
            )
        update("foo", listOf(Binding("version", `val`(1L)))).binding(listOf(Binding("xt/id")))
            .assertRoundTripTxOp(
                """{
                    "update": "foo",
                    "bind":[{"xt/id":{"xt:lvar":"xt/id"}}],
                    "set":[{"version":1}]
                    }
                """.trimJson()
            )

        update("foo", listOf(Binding("version", TRUE))).binding(listOf(Binding("xt/id")))
            .assertRoundTripTxOp(
                """{
                    "update": "foo",
                    "bind":[{"xt/id":{"xt:lvar":"xt/id"}}],
                    "set":[{"version":true}]
                    }
                """.trimJson()
            )
    }

    private fun Expr.assertRoundTripExpr(expectedJson: String) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(expectedJson, actualJson)
        assertEquals(this, JSON_SERDE.decodeFromString<Expr>(actualJson))
    }

    @Test
    fun shouldDeserializeExpr() {
        Null.assertRoundTripExpr("null")
        TRUE.assertRoundTripExpr("true")
        FALSE.assertRoundTripExpr("false")
        `val`("foo").assertRoundTripExpr(""""foo"""")
        `val`(Instant.parse("2023-01-01T12:34:56.789Z")).assertRoundTripExpr(
            """{"@type":"xt:instant","@value":"2023-01-01T12:34:56.789Z"}"""
        )
        lVar("foo").assertRoundTripExpr("""{"xt:lvar":"foo"}""")
        list(listOf(TRUE, FALSE)).assertRoundTripExpr("[true, false]".trimJson())
        set(listOf(TRUE, FALSE)).assertRoundTripExpr(
            """{
                "@type": "xt:set",
                "@value": [true, false]
                }
            """.trimJson()
        )
        map(mapOf("foo" to `val`(1L))).assertRoundTripExpr(
            """{
                "foo": 1
                }
            """.trimJson()
        )
        q(from("docs").bind("xt/id").build()).assertRoundTripExpr(
            """{
               "xt:q": 
                 {
                  "from": "docs",
                  "bind": [{"xt/id":{"xt:lvar":"xt/id"}}]
                 }
              }
           """.trimJson()
        )
    }

    private inline fun <reified T : Any> T.assertRoundTrip2(expectedJson: String) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(expectedJson, actualJson)
        assertEquals(this, JSON_SERDE.decodeFromString<T>(actualJson))
    }

    private inline fun <reified T : Any> T.assertRoundTrip2(inJson: String, outJson: String) {
        val actualJson = JSON_SERDE.encodeToString(this)
        assertEquals(outJson, actualJson)
        assertEquals(this, JSON_SERDE.decodeFromString<T>(inJson))
    }

    @Test
    fun shouldDeserializeQuery() {
        from("docs").bind("xt/id").build().assertRoundTrip2(
            """{
                "from": "docs",
                "bind": [{"xt/id":{"xt:lvar":"xt/id"}}]
               }
            """.trimJson()
        )
        from("docs")
            .bind("xt/id")
            .forValidTime(TemporalFilters.at(Instant.parse("2020-01-01T00:00:00Z")))
            .forSystemTime(TemporalFilter.AllTime)
            .build().assertRoundTrip2(
                """{
                "from": "docs",
                "bind": [{"xt/id":{"xt:lvar":"xt/id"}}],
                "forValidTime": {"at": {"@type":"xt:instant", "@value": "2020-01-01T00:00:00Z"}},
                "forSystemTime": "allTime"
               }
            """.trimJson()
            )
        pipeline(
            from("docs").bind("xt/id").build(),
            where(TRUE)
        ).assertRoundTrip2(
            """[{
                "from": "docs",
                "bind": [{"xt/id":{"xt:lvar":"xt/id"}}]
                },
                {"where": [true]}
               ]
            """.trimJson()
        )
        unify(
            from("docs").bind("xt/id").build(),
            from("docs").bind("xt/id").build()
        ).assertRoundTrip2(
            """{
                "unify": 
                  [{
                     "from": "docs",
                     "bind": [{"xt/id":{"xt:lvar":"xt/id"}}]
                   }, 
                   {
                     "from": "docs",
                     "bind": [{"xt/id":{"xt:lvar":"xt/id"}}]
                   }
                  ]
                }
            """.trimJson()
        )
        relation(param("foo"), Binding("xt/id", lVar("xt/id")))
            .assertRoundTrip2(
                """{
                    "rel": {"xt:param": "foo"},
                    "bind": [{"xt/id":{"xt:lvar":"xt/id"}}]
                   }
                """.trimJson()
            )
        relation(listOf(mapOf("xt/id" to `val`(1)), mapOf("xt/id" to `val`(2))), Binding("xt/id", lVar("xt/id")))
            .assertRoundTrip2(
                """{
                    "rel": [{"xt/id": 1}, {"xt/id": 2}],
                    "bind": [{"xt/id":{"xt:lvar":"xt/id"}}]
                   }
                """.trimJson()
            )

    }

    @Test
    fun shouldDeserializeQueryTail() {
        where(TRUE).assertRoundTrip2(
            """{"where":[true]}"""
        )
        limit(10).assertRoundTrip2(
            """{"limit":10}"""
        )
        offset(10).assertRoundTrip2(
            """{"offset":10}"""
        )
        orderBy(orderSpec(lVar("foo"))).assertRoundTrip2(
            """
                {"orderBy": ["foo"]}
            """.trimJson()
        )
        orderBy(orderSpec(lVar("foo")).desc().nullsLast()).assertRoundTrip2(
            """
                {"orderBy": [{"val": {"xt:lvar": "foo"}, "dir": "desc", "nulls": "last"}]}
            """.trimJson()
        )
        returning(listOf(Binding("a", lVar("a")), Binding("b", lVar("b")))).assertRoundTrip2(
            """{"return": ["a", {"b": {"xt:lvar": "b"}}]}""".trimJson(),
            """{"return": [{"a": {"xt:lvar": "a"}}, {"b": {"xt:lvar": "b"}}]}""".trimJson()
        )
        unnest(Binding("a", lVar("b"))).assertRoundTrip2(
            """{
                "unnest": {"a": {"xt:lvar": "b"}}
               }
            """.trimJson()
        )
        with().bindAll("a" to "b", "c" to "d").build().assertRoundTrip2(
            """{"with": [{"a": {"xt:lvar": "b"}}, {"c": {"xt:lvar": "d"}}]}""".trimJson()
        )
        without("a", "b").assertRoundTrip2(
            """{"without": ["a", "b"]}""".trimJson()
        )
        aggregate().bindAll("a" to "b", "c" to "d").build().assertRoundTrip2(
            """{
                "aggregate": [{"a": {"xt:lvar": "b"}}, {"c": {"xt:lvar": "d"}}]
               }
            """.trimJson()
        )
    }

    @Test
    fun shouldDeserializeUnify() {
        val from = from("docs").bind("xt/id").build()
        unify(
            from,
            where(TRUE),
            unnest(Binding("a")),
            with().bindAll("a" to "b", "c" to "d").build(),
            join(from),
            leftJoin(from),
            relation(param("foo"), Binding("xt/id", lVar("xt/id")))
        ).assertRoundTrip2(
            """{
                "unify": [{"from":"docs",
                           "bind":[{"xt/id":{"xt:lvar":"xt/id"}}]},
                          {"where":[true]},
                          {"unnest":{"a":{"xt:lvar":"a"}}},
                          {"with":[{"a":{"xt:lvar":"b"}},{"c":{"xt:lvar":"d"}}]},
                          {"join":{"from":"docs","bind":[{"xt/id":{"xt:lvar":"xt/id"}}]}},
                          {"leftJoin":{"from":"docs","bind":[{"xt/id":{"xt:lvar":"xt/id"}}]}},
                          {"rel":{"xt:param":"foo"},"bind":[{"xt/id":{"xt:lvar":"xt/id"}}]}]
               }
            """.trimJson()
        )
    }

    @Test
    fun shouldDeserializeQueryRequest() {
        val txKey = txKey(1, Instant.EPOCH)
        QueryRequest(
            from("docs").bind("xt/id").build(),
            queryOpts()
                .args(mapOf("foo" to "bar"))
                .basis(Basis(txKey, Instant.EPOCH))
                .afterTx(txKey)
                .txTimeout(Duration.parse("PT3H"))
                .defaultTz(ZoneId.of("America/Los_Angeles"))
                .defaultAllValidTime(true)
                .explain(true)
                .keyFn(IKeyFn.KeyFn.KEBAB_CASE_KEYWORD)
                .build()
        ).assertRoundTrip2(
            """{
                "query": {"from":"docs","bind":[{"xt/id":{"xt:lvar":"xt/id"}}]},
                "queryOpts": {"args":{"foo":"bar"},
                              "basis":{"atTx":{"txId":1,"systemTime":"1970-01-01T00:00:00Z"},
                                       "currentTime": "1970-01-01T00:00:00Z"},
                              "afterTx":{"txId":1,"systemTime":"1970-01-01T00:00:00Z"},
                              "txTimeout":"PT3H",
                              "defaultTz":"America/Los_Angeles",
                              "defaultAllValidTime":true,
                              "explain":true,
                              "keyFn":"KEBAB_CASE_KEYWORD"}
              }
            """.trimJson()
        )
    }
}
