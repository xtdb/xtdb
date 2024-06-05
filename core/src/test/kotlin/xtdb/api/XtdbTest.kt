package xtdb.api

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
import xtdb.api.query.queryOpts
import xtdb.api.tx.TxOps.putDocs
import xtdb.api.tx.TxOps.sql
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
        node.executeTx(putDocs("foo", mapOf("xt\$id" to "jms")))

        assertEquals(
            listOf(mapOf("foo_id" to "jms")),

            node.openQuery("SELECT foo.xt\$id AS foo_id FROM foo").doall()
        )
    }

    private val emptyRel = relation(listOf(emptyMap()), emptyList())

    @Test
    fun `test query opts`() {
        node.executeTx(putDocs("docs2", mapOf("xt\$id" to 1, "foo" to "bar")))

        assertEquals(
            listOf(mapOf("my_foo" to "bar")),
            node.openQuery("SELECT foo AS my_foo FROM docs2").doall(),
        )


        assertEquals(
            listOf(mapOf("my-foo" to "bar")),
            node.openQuery("SELECT foo AS myFoo FROM docs2",
                QueryOptions(keyFn = KEBAB_CASE_STRING)
            ).doall(),

            "key-fn"
        )

        assertEquals(
            listOf(mapOf("foo" to "bar")),
            node.openQuery(
                "SELECT foo FROM docs2 WHERE xt\$id = ?",
                queryOpts().args(listOf(1)).build()
            ).doall(),

            "args"
        )

        assertEquals(
            listOf(mapOf("current_time" to LocalDate.parse("2020-01-01"))),

            node.openQuery(
                "SELECT CURRENT_DATE AS currentTime",
                QueryOptions(basis = Basis(currentTime = Instant.parse("2020-01-01T12:34:56.000Z")))
            ).doall(),

            "current-time"
        )

        assertEquals(
            listOf(mapOf("timestamp" to ZonedDateTime.parse("2020-01-01T04:34:56-08:00[America/Los_Angeles]"))),

            node.openQuery(
                "SELECT CURRENT_TIMESTAMP AS `timestamp`",
                QueryOptions(
                    basis = Basis(currentTime = Instant.parse("2020-01-01T12:34:56.000Z")),
                    defaultTz = ZoneId.of("America/Los_Angeles")
                )
            ).doall(),

            "default-tz"
        )

        val plan = "[:project\n [{foo docs.1/foo}]\n [:rename docs.1 [:scan {:table docs} [foo]]]]\n"

        node.submitTx(sql("INSERT INTO docs (xt\$id, foo) VALUES (1, 'bar')"))

        assertEquals(
            listOf(mapOf("plan" to plan)),

            node.openQuery(
                "SELECT foo FROM docs",
                QueryOptions(explain = true)
            ).doall(),

            "explain"
        )
    }
}
