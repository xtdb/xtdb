package xtdb.mysql

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.api.error.Incorrect
import xtdb.api.error.Unavailable
import xtdb.api.error.Unsupported
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class MySqlWireDriverTest : MySqlSourceTestBase() {
    private lateinit var db: String

    @BeforeEach
    fun setUp() {
        db = unique("drv")
        mysqlExecute("CREATE DATABASE $db", "CREATE TABLE $db.docs (_id INT PRIMARY KEY, v INT)", "CREATE TABLE $db.other (_id INT PRIMARY KEY)")
    }

    @AfterEach
    fun tearDown() {
        mysqlExecute(
            "SET GLOBAL binlog_row_image = 'FULL'", "SET GLOBAL binlog_transaction_compression = OFF",
            "DROP DATABASE IF EXISTS $db",
        )
    }

    private fun driver(vararg tables: String, port: Int = upstream.port) = MySqlWireDriver(
        "test", upstream.host, port, upstream.user, upstream.password, serverId(),
        tables.map { TableId(db, it) }.toSet(), 1.seconds,
    )

    private fun executed() = GtidSet.parse(mysqlQuery("SELECT @@global.gtid_executed AS g")[0]["g"] as String)

    private fun pollOne(stream: MySqlDriver.ChangeStream): MySqlDriver.Transaction.Committed = runBlocking {
        var tx: MySqlDriver.Transaction.Committed? = null
        val deadline = System.currentTimeMillis() + 20_000
        while (tx == null && System.currentTimeMillis() < deadline) tx = stream.poll()
        assertNotNull(tx, "a transaction arrives")
        tx!!
    }

    @Test
    fun `names the setting that is wrong`() {
        driver("docs").checkUpstream()

        mysqlExecute("SET GLOBAL binlog_row_image = 'MINIMAL'")
        val e = assertThrows(Incorrect::class.java) { driver("docs").checkUpstream() }
        assertTrue("binlog_row_image" in e.message!!, e.message)

        mysqlExecute("SET GLOBAL binlog_row_image = 'FULL'")
        val missing = assertThrows(Incorrect::class.java) { driver("nope").checkUpstream() }
        assertEquals("xtdb.mysql/missing-table", missing.errorCode())
    }

    @Test
    fun `refuses to start past binlog the upstream has purged`() = runBlocking {
        val position = executed()
        mysqlExecute("INSERT INTO $db.docs VALUES (1, 1)")
        mysqlExecute("FLUSH BINARY LOGS")
        val current = mysqlQuery("SHOW MASTER STATUS")[0]["File"]
        mysqlExecute("PURGE BINARY LOGS TO '$current'")

        val e = assertThrows(Incorrect::class.java) { runBlocking { driver("docs").openStream(position) } }
        assertEquals("xtdb.mysql/binlog-purged", e.errorCode())

        driver("docs").openStream(executed()).close()
    }

    @Test
    fun `refuses a position the upstream hasn't reached`() = runBlocking {
        val ahead = executed() + Gtid(java.util.UUID.randomUUID(), 1)
        val e = assertThrows(Incorrect::class.java) { runBlocking { driver("docs").openStream(ahead) } }
        assertEquals("xtdb.mysql/upstream-behind", e.errorCode())
    }

    @Test
    fun `a stream hands over one transaction, with only the included tables' rows, and skips what the position holds`() = runBlocking {
        val start = executed()
        mysqlTx("INSERT INTO $db.docs VALUES (1, 1)", "INSERT INTO $db.other VALUES (1)", "UPDATE $db.docs SET v = 2 WHERE _id = 1", "DELETE FROM $db.docs WHERE _id = 1")
        mysqlExecute("CREATE TABLE $db.ddl (x INT)")
        mysqlExecute("INSERT INTO $db.other VALUES (2)")

        driver("docs").openStream(start).use { stream ->
            val first = pollOne(stream)
            assertEquals(listOf("Put", "Update", "Delete"), first.ops.map { it::class.simpleName })
            assertEquals(listOf("_id"), first.ops[0].primaryKey)
            assertEquals(mapOf("_id" to 1L, "v" to 1L), (first.ops[1] as RowOp.Update).before)

            val ddl = pollOne(stream)
            assertTrue(ddl.ops.isEmpty(), "DDL is a transaction of its own with nothing in it")

            val skipped = pollOne(stream)
            assertTrue(skipped.ops.isEmpty())
            assertEquals(executed(), skipped.position)
        }

        driver("docs").openStream(start).use { stream ->
            val first = pollOne(stream)
            driver("docs").openStream(first.position).use { again -> assertTrue(pollOne(again).ops.isEmpty(), "the first transaction isn't presented again") }
        }
    }

    @Test
    fun `row changes carry the table's primary-key columns in key order, from the snapshot and the binlog`() = runBlocking {
        mysqlExecute("CREATE TABLE $db.keyed (a INT, b INT, v INT, PRIMARY KEY (b, a))", "INSERT INTO $db.keyed VALUES (1, 2, 3)")

        driver("keyed").openSnapshot().use { snapshot ->
            assertEquals(listOf("b", "a"), snapshot.batches().first().single().primaryKey)
        }

        val start = executed()
        mysqlExecute("INSERT INTO $db.keyed VALUES (4, 5, 6)")
        driver("keyed").openStream(start).use { assertEquals(listOf("b", "a"), pollOne(it).ops.single().primaryKey) }
    }

    @Test
    fun `an idle stream stays open on the upstream's heartbeats`() = runBlocking {
        driver("docs").openStream(executed()).use { stream ->
            val until = System.currentTimeMillis() + 5_000
            while (System.currentTimeMillis() < until) assertEquals(null, stream.poll())
        }
    }

    @Test
    fun `a connection that goes silent is reported lost`() = runBlocking {
        FreezableProxy(upstream.host, upstream.port).use { proxy ->
            driver("docs", port = proxy.port).openStream(executed()).use { stream ->
                stream.poll()
                proxy.frozen = true

                val e = assertThrows(Unavailable::class.java) {
                    runBlocking { repeat(200) { stream.poll() } }
                }
                assertEquals("xtdb.mysql/binlog-connection-lost", e.errorCode())
            }
        }
    }

    @Test
    fun `compressed transactions are refused, not silently dropped`() = runBlocking {
        mysqlExecute("SET GLOBAL binlog_transaction_compression = ON")
        assertEquals("xtdb.mysql/upstream-setting", assertThrows(Incorrect::class.java) { driver("docs").checkUpstream() }.errorCode())

        val start = executed()
        mysqlTx("INSERT INTO $db.docs VALUES (5, 5)")

        val e = assertThrows(Unsupported::class.java) { runBlocking { driver("docs").openStream(start).use { pollOne(it) } } }
        assertEquals("xtdb.mysql/compressed-transaction", e.errorCode())
    }
}
