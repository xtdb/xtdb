package xtdb.mysql

import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class MySqlSourceIndexerTest : MySqlSourceTestBase() {

    @Test
    fun `custom indexer reroutes, drops a column and derives _id from the primary key`() = runTest(timeout = 120.seconds) {
        val db = unique("idx")
        val tmp = Files.createTempDirectory("mysql-reroute")

        mysqlExecute(
            "CREATE DATABASE $db",
            "CREATE TABLE $db.widgets (sku VARCHAR(10) PRIMARY KEY, name VARCHAR(20), secret VARCHAR(20))",
            "INSERT INTO $db.widgets VALUES ('a1', 'anvil', 'classified')",
        )

        val node = openNode(tmp.resolve("log"), tmp.resolve("storage"))
        try {
            attachCdc(
                node, "cdc", tmp.resolve("cdc-log"), tmp.resolve("cdc-storage"), serverId(), listOf("$db.widgets"),
                indexer = "!TestReroute {target: gadgets, dropColumn: secret}",
            )
            awaitStreaming(node)
            mysqlExecute("INSERT INTO $db.widgets VALUES ('b2', 'bolt', 'hush')", "UPDATE $db.widgets SET name = 'big anvil' WHERE sku = 'a1'")

            val gadgets = eventually(30.seconds) {
                xtQuery(node, "cdc", "SELECT _id, name FROM rerouted.gadgets ORDER BY _id").also { assertEquals(2, it.size) }
            }
            assertEquals(listOf("a1" to "big anvil", "b2" to "bolt"), gadgets.map { it["_id"] to it["name"] })
            assertFalse(xtQuery(node, "cdc", "SELECT * FROM rerouted.gadgets").any { it.containsKey("secret") })
        } finally {
            node.close()
            mysqlExecute("DROP DATABASE IF EXISTS $db")
        }
    }
}
