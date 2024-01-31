package xtdb.api

import clojure.java.api.Clojure
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.query.Basis
import xtdb.api.query.Exprs.expr
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_STRING
import xtdb.api.query.Queries.from
import xtdb.api.query.Queries.pipeline
import xtdb.api.query.Queries.relation
import xtdb.api.query.Queries.with
import xtdb.api.query.QueryOptions
import xtdb.api.tx.TxOps.putDocs
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.stream.Stream

internal class XtdbTest {
    private lateinit var node: IXtdb

    @BeforeEach
    fun setUp() {
        node = Xtdb.openNode()
    }

    @AfterEach
    fun tearDown() {
        node.close()
    }

    private fun Stream<*>.doall() = use { toList() }

    @Test
    fun startsInMemoryNode() {
        node.submitTx(putDocs("foo", mapOf("xt\$id" to "jms")))

        assertEquals(
            listOf(mapOf("id" to "jms")),

            node.openQuery(
                from("foo") {
                    bindAll("xt\$id" to "id")
                }
            ).doall()
        )

        assertEquals(
            listOf(mapOf("foo_id" to "jms")),

            node.openQuery("SELECT foo.xt\$id AS foo_id FROM foo").doall()
        )
    }

    private val emptyRel = relation(listOf(emptyMap()), emptyList())

    @Test
    fun `test query opts`() {
        node.submitTx(putDocs("docs2", mapOf("xt\$id" to 1, "foo" to "bar")))

        assertEquals(
            listOf(mapOf("myFoo" to "bar")),
            node.openQuery(
                from("docs2") {
                    bindAll("foo" to "myFoo")
                }
            ).doall(),

            "Java AST queries"
        )


        assertEquals(
            listOf(mapOf("my-foo" to "bar")),
            node.openQuery(
                from("docs2") {
                    bindAll("foo" to "my_foo")
                },

                QueryOptions(keyFn = KEBAB_CASE_STRING)
            ).doall(),

            "key-fn"
        )

        assertEquals(
            listOf(mapOf("foo" to "bar")),
            node.openQuery(
                from("docs2") {
                    bindAll("xt\$id" to expr { "\$id".param })
                    bindAll("foo")
                },
                QueryOptions(args = mapOf("id" to 1))
            ).doall(),

            "args"
        )

        assertEquals(
            listOf(mapOf("currentTime" to LocalDate.parse("2020-01-01"))),

            node.openQuery(
                pipeline(
                    emptyRel,
                    with { bindAll("currentTime" to expr { "currentDate"() }) }
                ),

                QueryOptions(basis = Basis(currentTime = Instant.parse("2020-01-01T12:34:56.000Z")))
            ).doall(),

            "current-time"
        )

        assertEquals(
            listOf(mapOf("timestamp" to ZonedDateTime.parse("2020-01-01T04:34:56-08:00[America/Los_Angeles]"))),

            node.openQuery(
                pipeline(
                    emptyRel,
                    with { bindAll("timestamp" to expr { "currentTimestamp"() }) }
                ),

                QueryOptions(
                    basis = Basis(currentTime = Instant.parse("2020-01-01T12:34:56.000Z")),
                    defaultTz = ZoneId.of("America/Los_Angeles")
                )
            ).doall(),

            "default-tz"
        )

        val plan = Clojure.read(
            """
               [:scan {:table docs, :for-valid-time nil, :for-system-time nil}
                 [foo]] 
            """.trimIndent()
        )

        assertEquals(
            listOf(mapOf("plan" to plan)),

            node.openQuery(
                from("docs") { bind("foo") },
                QueryOptions(explain = true)
            ).doall(),

            "explain"
        )
    }
}
