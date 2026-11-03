package xtdb.postgres

import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import java.nio.file.Files
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class PostgresSourceTruncateTest : PostgresSourceTestBase() {

    private fun ingestionError(node: Xtdb) = (node as XtdbInternal).dbCatalog["cdc"]?.ingestionError

    private suspend fun withCdc(setup: List<String>, published: List<String>, body: suspend (Xtdb) -> Unit) {
        val pub = unique("pub"); val slot = unique("slot")
        val dirs = List(4) { Files.createTempDirectory("pg-trunc") }
        pgExecute(*setup.toTypedArray(), "CREATE PUBLICATION $pub FOR TABLE ${published.joinToString()}")
        val node = openNode(dirs[0], dirs[1])
        try {
            attachCdc(node, database = "cdc", cdcLog = dirs[2], cdcStorage = dirs[3], slot = slot, pub = pub)
            awaitStreaming(node)
            body(node)
        } finally {
            node.close()
            runCatching { dropSlot(slot) }
            dirs.forEach { it.toFile().deleteRecursively() }
        }
    }

    private fun pgTx(vararg statements: String) =
        pgConn().use { c ->
            c.autoCommit = false
            c.createStatement().use { s -> statements.forEach(s::execute) }
            c.commit()
        }

    private fun ids(node: Xtdb, table: String, forAllValidTime: Boolean = false) =
        xtQuery(node, "cdc", "SELECT _id FROM public.$table ${if (forAllValidTime) "FOR ALL VALID_TIME" else ""} ORDER BY _id")
            .map { (it["_id"] as Number).toInt() }

    private suspend fun awaitIds(node: Xtdb, table: String, expected: List<Int>, forAllValidTime: Boolean = false) =
        eventually(30.seconds) { assertEquals(expected, ids(node, table, forAllValidTime)) }

    @Test
    fun `insert, truncate, insert in one tx leaves only the last insert`() = runTest(timeout = 5.minutes) {
        val cur = unique("pg"); val vf = unique("pg")
        withCdc(
            listOf("CREATE TABLE $cur (_id INT PRIMARY KEY)", "CREATE TABLE $vf (_id INT PRIMARY KEY, _valid_from TIMESTAMPTZ)"),
            listOf(cur, vf),
        ) { node ->
            pgTx(
                "INSERT INTO $cur VALUES (1), (2)", "TRUNCATE $cur", "INSERT INTO $cur VALUES (1)",
                "INSERT INTO $vf VALUES (1, '2020-01-01Z'), (2, '2020-01-01Z')", "TRUNCATE $vf", "INSERT INTO $vf VALUES (1, '2020-01-01Z')",
            )
            awaitIds(node, cur, listOf(1))
            awaitIds(node, vf, listOf(1), forAllValidTime = true)
        }
    }

    @Test
    fun `truncate of a mixed-case table empties it`() = runTest(timeout = 5.minutes) {
        val t = unique("Pg")
        withCdc(listOf("CREATE TABLE \"$t\" (_id INT PRIMARY KEY)"), listOf("\"$t\"")) { node ->
            pgExecute("INSERT INTO \"$t\" VALUES (1)")
            awaitIds(node, "\"$t\"", listOf(1))

            pgExecute("TRUNCATE \"$t\"")

            awaitIds(node, "\"$t\"", emptyList())
        }
    }

    @Test
    fun `truncate removes a _valid_from table's history, and leaves a current-state table's history before it`() = runTest(timeout = 5.minutes) {
        val vf = unique("pg"); val cur = unique("pg")
        withCdc(
            listOf(
                "CREATE TABLE $vf (_id INT PRIMARY KEY, _valid_from TIMESTAMPTZ, v TEXT)",
                "CREATE TABLE $cur (_id INT PRIMARY KEY, v TEXT)",
            ),
            listOf(vf, cur),
        ) { node ->
            pgExecute(
                "INSERT INTO $vf VALUES (1, '2020-01-01Z', 'a')",
                "UPDATE $vf SET _valid_from = '2021-01-01Z', v = 'b' WHERE _id = 1",
                "INSERT INTO $cur VALUES (1, 'a')",
                "UPDATE $cur SET v = 'b' WHERE _id = 1",
            )
            eventually(30.seconds) {
                assertEquals(listOf(1, 1), ids(node, vf, forAllValidTime = true))
                assertEquals(listOf(1, 1), ids(node, cur, forAllValidTime = true))
            }

            pgExecute("TRUNCATE $vf, $cur")

            awaitIds(node, vf, emptyList(), forAllValidTime = true)
            awaitIds(node, cur, emptyList())
            assertEquals(listOf(1, 1), ids(node, cur, forAllValidTime = true))
        }
    }

    @Test
    fun `one truncate of several tables empties each`() = runTest(timeout = 5.minutes) {
        val a = unique("pg"); val b = unique("pg"); val marker = unique("pg")
        withCdc(
            listOf("CREATE TABLE $a (_id INT PRIMARY KEY)", "CREATE TABLE $b (_id INT PRIMARY KEY)", "CREATE TABLE $marker (_id INT PRIMARY KEY)"),
            listOf(a, b, marker),
        ) { node ->
            pgExecute("INSERT INTO $a VALUES (1)", "INSERT INTO $b VALUES (1)")
            awaitIds(node, b, listOf(1))

            pgExecute("TRUNCATE $a, $b", "INSERT INTO $marker VALUES (1)")

            awaitIds(node, marker, listOf(1))
            assertEquals(emptyList<Int>(), ids(node, a))
            assertEquals(emptyList<Int>(), ids(node, b))
        }
    }

    @Test
    fun `truncate cascade across an FK chain empties the referencing table`() = runTest(timeout = 5.minutes) {
        val parent = unique("pg"); val child = unique("pg")
        withCdc(
            listOf(
                "CREATE TABLE $parent (_id INT PRIMARY KEY)",
                "CREATE TABLE $child (_id INT PRIMARY KEY, p INT REFERENCES $parent (_id))",
            ),
            listOf(parent, child),
        ) { node ->
            pgExecute("INSERT INTO $parent VALUES (1)", "INSERT INTO $child VALUES (10, 1)")
            awaitIds(node, child, listOf(10))

            pgExecute("TRUNCATE $parent CASCADE")

            awaitIds(node, child, emptyList())
            awaitIds(node, parent, emptyList())
        }
    }

    @Test
    fun `a truncate of a table XT has never written stops ingestion with Table not found`() = runTest(timeout = 5.minutes) {
        val t = unique("pg")
        withCdc(listOf("CREATE TABLE $t (_id INT PRIMARY KEY)"), listOf(t)) { node ->
            pgExecute("TRUNCATE $t")

            eventually(30.seconds) {
                val error = assertNotNull(ingestionError(node))
                assertTrue("Table not found" in error.message.orEmpty(), error.message)
            }
        }
    }
}
