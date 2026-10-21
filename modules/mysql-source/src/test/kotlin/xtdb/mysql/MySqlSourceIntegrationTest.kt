package xtdb.mysql

import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import kotlin.concurrent.thread
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class MySqlSourceIntegrationTest : MySqlSourceTestBase() {

    private lateinit var db: String
    private lateinit var tmp: Path
    private var node: Xtdb? = null

    @BeforeEach
    fun setUp() {
        db = unique("shop")
        tmp = Files.createTempDirectory("mysql-source")
        mysqlExecute("CREATE DATABASE $db")
    }

    @AfterEach
    fun tearDown() {
        node?.close()
        mysqlExecute("DROP DATABASE IF EXISTS $db")
    }

    private fun open(): Xtdb = openNode(tmp.resolve("log"), tmp.resolve("storage")).also { node = it }

    private fun attach(node: Xtdb, vararg tables: String, serverId: Long = serverId()) =
        attachCdc(node, "cdc", tmp.resolve("cdc-log"), tmp.resolve("cdc-storage"), serverId, tables.map { "$db.$it" })

    private fun q(sql: String) = xtQuery(node!!, "cdc", sql)

    private fun instant(v: Any?): Instant = when (v) {
        is java.sql.Timestamp -> v.toInstant()
        is OffsetDateTime -> v.toInstant()
        is ZonedDateTime -> v.toInstant()
        is Instant -> v
        else -> error("not a timestamp: $v")
    }

    @Test
    fun `snapshots what is there, then streams inserts, updates and deletes`() = runTest(timeout = 120.seconds) {
        mysqlExecute(
            "CREATE TABLE $db.docs (_id INT PRIMARY KEY, v VARCHAR(20))",
            "INSERT INTO $db.docs VALUES (1, 'one'), (2, 'two')",
        )

        val node = open()
        attach(node, "docs")
        awaitStreaming(node)

        assertEquals(listOf(mapOf("_id" to 1L, "v" to "one"), mapOf("_id" to 2L, "v" to "two")), q("SELECT _id, v FROM $db.docs ORDER BY _id"))

        mysqlExecute(
            "INSERT INTO $db.docs VALUES (3, 'three')",
            "UPDATE $db.docs SET v = 'uno' WHERE _id = 1",
            "DELETE FROM $db.docs WHERE _id = 2",
        )

        eventually(30.seconds) {
            assertEquals(listOf(mapOf("_id" to 1L, "v" to "uno"), mapOf("_id" to 3L, "v" to "three")), q("SELECT _id, v FROM $db.docs ORDER BY _id"))
        }
    }

    @Test
    fun `one upstream transaction is one XT transaction, across tables`() = runTest(timeout = 120.seconds) {
        mysqlExecute(
            "CREATE TABLE $db.a (_id INT PRIMARY KEY, v INT)",
            "CREATE TABLE $db.b (_id INT PRIMARY KEY, v INT)",
        )

        val node = open()
        attach(node, "a", "b")
        awaitStreaming(node)

        mysqlTx("INSERT INTO $db.a VALUES (1, 10)", "INSERT INTO $db.b VALUES (1, 20)", "INSERT INTO $db.a VALUES (2, 30)")
        mysqlTx("INSERT INTO $db.b VALUES (2, 40)")

        eventually(30.seconds) {
            assertEquals(2, q("SELECT _id FROM $db.a").size)
            assertEquals(2, q("SELECT _id FROM $db.b").size)
        }

        val a = q("SELECT _id, _system_from FROM $db.a ORDER BY _id").map { it["_id"] to instant(it["_system_from"]) }
        val b = q("SELECT _id, _system_from FROM $db.b ORDER BY _id").map { it["_id"] to instant(it["_system_from"]) }

        assertEquals(a[0].second, a[1].second, "both of a's rows are from the one tx")
        assertEquals(a[0].second, b[0].second, "and so is b's row from it")
        assertNotEquals(a[0].second, b[1].second, "while the next upstream transaction is the next XT one")
        assertTrue(a[0].second < b[1].second)
    }

    @Test
    fun `system time is the upstream commit time, to the microsecond`() = runTest(timeout = 120.seconds) {
        mysqlExecute("CREATE TABLE $db.docs (_id INT PRIMARY KEY)")

        val node = open()
        attach(node, "docs")
        awaitStreaming(node)

        val windows = (1..5).map { i ->
            val before = Instant.now()
            mysqlExecute("INSERT INTO $db.docs VALUES ($i)")
            before to Instant.now()
        }

        eventually(30.seconds) { assertEquals(5, q("SELECT _id FROM $db.docs").size) }

        val systemTimes = q("SELECT _id, _system_from FROM $db.docs ORDER BY _id").map { instant(it["_system_from"]) }

        systemTimes.zip(windows).forEach { (st, window) ->
            assertTrue(st >= window.first.minusSeconds(1) && st <= window.second.plusSeconds(1), "$st within ${window.first}..${window.second}")
        }

        assertTrue(systemTimes.any { it.nano % 1_000_000 != 0 }, "not truncated to the binlog header's whole seconds, nor to millis: $systemTimes")
    }

    @Test
    fun `resumes after a restart without re-importing what it had`() = runTest(timeout = 180.seconds) {
        mysqlExecute(
            "CREATE TABLE $db.docs (_id INT PRIMARY KEY, v INT)",
            "INSERT INTO $db.docs VALUES (1, 1)",
        )

        val first = open()
        val sid = serverId()
        attach(first, "docs", serverId = sid)
        awaitStreaming(first)
        mysqlExecute("INSERT INTO $db.docs VALUES (2, 2)")
        eventually(30.seconds) { assertEquals(2, q("SELECT _id FROM $db.docs").size) }

        first.close()

        mysqlExecute("INSERT INTO $db.docs VALUES (3, 3)", "UPDATE $db.docs SET v = 20 WHERE _id = 2")

        val second = open()
        awaitStreaming(second)

        eventually(30.seconds) {
            assertEquals(listOf(1L to 1L, 2L to 20L, 3L to 3L), q("SELECT _id, v FROM $db.docs ORDER BY _id").map { it["_id"] to it["v"] })
        }

        assertEquals(
            listOf(1L to 1, 2L to 2, 3L to 1),
            q("SELECT _id, COUNT(*) AS n FROM $db.docs FOR ALL SYSTEM_TIME GROUP BY _id ORDER BY _id").map { it["_id"] to (it["n"] as Number).toInt() },
            "system-time versions per row",
        )
    }

    @Test
    fun `transactions on other tables, and DDL, still advance the position`() = runTest(timeout = 120.seconds) {
        mysqlExecute(
            "CREATE TABLE $db.docs (_id INT PRIMARY KEY)",
            "CREATE TABLE $db.other (_id INT PRIMARY KEY)",
        )

        val node = open()
        attach(node, "docs")
        awaitStreaming(node)

        mysqlExecute(
            "INSERT INTO $db.other VALUES (1)",
            "CREATE TABLE $db.ddl_only (x INT)",
            "INSERT INTO $db.other VALUES (2)",
        )

        val executed = GtidSet.parse(mysqlQuery("SELECT @@global.gtid_executed AS g")[0]["g"] as String)

        eventually(30.seconds) {
            assertTrue(executed.isSubsetOf(appliedPosition(node)!!), "applied ${appliedPosition(node)} should cover $executed")
        }

        mysqlExecute("INSERT INTO $db.docs VALUES (7)")
        eventually(30.seconds) { assertEquals(listOf(7L), q("SELECT _id FROM $db.docs").map { it["_id"] }) }
    }

    @Test
    fun `an update that changes _id ends the row under the old one`() = runTest(timeout = 120.seconds) {
        mysqlExecute(
            "CREATE TABLE $db.docs (_id INT PRIMARY KEY, v VARCHAR(5))",
            "INSERT INTO $db.docs VALUES (1, 'a')",
        )

        val node = open()
        attach(node, "docs")
        awaitStreaming(node)

        mysqlExecute("UPDATE $db.docs SET _id = 2 WHERE _id = 1")

        eventually(30.seconds) { assertEquals(listOf(mapOf("_id" to 2L, "v" to "a")), q("SELECT _id, v FROM $db.docs")) }
    }

    @Test
    fun `adding a column upstream shows up in rows from then on`() = runTest(timeout = 120.seconds) {
        mysqlExecute(
            "CREATE TABLE $db.docs (_id INT PRIMARY KEY, a INT)",
            "INSERT INTO $db.docs VALUES (1, 1)",
        )

        val node = open()
        attach(node, "docs")
        awaitStreaming(node)

        mysqlExecute("ALTER TABLE $db.docs ADD COLUMN b VARCHAR(5)", "INSERT INTO $db.docs VALUES (2, 2, 'bee')")

        eventually(30.seconds) {
            assertEquals("bee", q("SELECT b FROM $db.docs WHERE _id = 2").singleOrNull()?.get("b"))
        }
    }

    @Test
    fun `the snapshot is consistent with writes landing while it runs`() = runTest(timeout = 300.seconds) {
        mysqlExecute("CREATE TABLE $db.docs (_id INT PRIMARY KEY, pad VARCHAR(100))")
        mysqlTx(*(1..20_000).chunked(1000).map { chunk ->
            "INSERT INTO $db.docs VALUES " + chunk.joinToString { "($it, 'padding-$it')" }
        }.toTypedArray())

        val node = open()

        val next = java.util.concurrent.atomic.AtomicInteger(20_001)
        val writing = java.util.concurrent.atomic.AtomicBoolean(true)
        val writer = thread {
            mysqlConn().use { c ->
                c.createStatement().use { s ->
                    while (writing.get()) { s.execute("INSERT INTO $db.docs VALUES (${next.getAndIncrement()}, 'live')") }
                }
            }
        }

        attach(node, "docs")
        awaitStreaming(node)
        Thread.sleep(1000)
        writing.set(false)
        writer.join()

        val total = (mysqlQuery("SELECT COUNT(*) AS n FROM $db.docs")[0]["n"] as Number).toInt()
        assertTrue(total > 20_000, "the writer got some in ($total)")

        eventually(60.seconds) { assertEquals(total, (q("SELECT COUNT(*) AS n FROM $db.docs")[0]["n"] as Number).toInt()) }

        assertEquals(total, (q("SELECT COUNT(*) AS n FROM $db.docs FOR ALL SYSTEM_TIME")[0]["n"] as Number).toInt(), "one version per row")
    }
}
