package xtdb.api

import clojure.java.api.Clojure
import clojure.lang.Keyword
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.query.*
import xtdb.api.query.Binding.Companion.bindVar
import xtdb.api.query.Expr.Companion.call
import xtdb.api.query.IKeyFn.KeyFn.CLOJURE_KW
import xtdb.api.query.IKeyFn.KeyFn.CLOJURE_STR
import xtdb.api.query.Query.Companion.from
import xtdb.api.query.Query.Companion.pipeline
import xtdb.api.query.Query.Companion.relation
import xtdb.api.query.Query.Companion.withCols
import xtdb.api.tx.TxOp.Companion.put
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.stream.Stream

internal class XtdbTest {
    private lateinit var node: IXtdb

    @BeforeEach
    fun setUp() {
        node = Xtdb.startNode()
    }

    @AfterEach
    fun tearDown() {
        node.close()
    }

    private fun Stream<*>.doall() = use { toList() }

    @Test
    fun startsInMemoryNode() {
        node.submitTx(put("foo", mapOf("xt/id" to "jms")))

        assertEquals(
            listOf(mapOf("id" to "jms")),

            node.openQuery(
                from("foo")
                    .binding(listOf("xt/id" toVar "id"))
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
        node.submitTx(put("docs2", mapOf("xt/id" to 1, "foo" to "bar")))

        assertEquals(
            listOf(mapOf("my_foo" to "bar")),
            node.openQuery(
                from("docs2")
                    .binding(listOf("foo" toVar "my_foo"))
            ).doall(),

            "Java AST queries"
        )


        assertEquals(
            listOf(mapOf("my-foo" to "bar")),
            node.openQuery(
                from("docs2")
                    .binding(listOf("foo" toVar "my_foo")),

                QueryOptions(keyFn = CLOJURE_STR)
            ).doall(),

            "key-fn"
        )

        assertEquals(
            listOf(mapOf("foo" to "bar")),
            node.openQuery(
                from("docs2")
                    .binding(listOf("xt/id" toParam "\$id", bindVar("foo"))),
                QueryOptions(args = mapOf("id" to 1))
            ).doall(),

            "args"
        )

        assertEquals(
            listOf(mapOf("current_time" to LocalDate.parse("2020-01-01"))),

            node.openQuery(
                pipeline(
                    emptyRel,
                    listOf(withCols(listOf(Binding("current-time", call("current-date", emptyList())))))
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
                    listOf(withCols(listOf(Binding("timestamp", call("current-timestamp", emptyList())))))
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
                from("docs")
                    .binding(listOf(bindVar("foo"))),
                QueryOptions(explain = true)
            ).doall(),

            "explain"
        )
    }
}