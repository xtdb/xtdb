package xtdb.postgres

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import java.nio.file.Files
import kotlin.time.Duration.Companion.seconds

/**
 * End-to-end proof of the pluggable-indexer seam: a genuinely-registered, test-only [PgIndexer]
 * ([TestRerouteIndexer], `!TestReroute`) selected through ATTACH YAML re-routes rows to a different
 * table and drops a column on the way through. Exercises the whole new pathway — YAML deserialize,
 * proto persist/restore, `open`, and `indexTx(tx, openTx)` — against a real Postgres source.
 */
@Tag("integration")
class PostgresSourceIndexerTest : PostgresSourceTestBase() {

    private fun attachCdcWithReroute(
        node: Xtdb,
        cdcLog: java.nio.file.Path,
        cdcStorage: java.nio.file.Path,
        slot: String,
        pub: String,
        target: String,
        dropColumn: String,
    ) {
        node.createConnectionBuilder().build().use { c ->
            c.createStatement().use { s ->
                s.execute(
                    """
                    ATTACH DATABASE cdc WITH $$
                        storage: !Local
                          path: $cdcStorage
                        log: !Local
                          path: $cdcLog
                        externalSource: !Postgres
                          remote: pg
                          database: testdb
                          slotName: $slot
                          publicationName: $pub
                          indexer: !TestReroute
                            target: $target
                            dropColumn: $dropColumn
                    $$""".trimIndent()
                )
            }
        }
    }

    @Test
    fun `custom indexer reroutes table and drops column`() = runTest(timeout = 120.seconds) {
        val slot = unique("reroute_slot")
        val pub = unique("reroute_pub")
        val tmp = Files.createTempDirectory("pg-reroute")

        pgExecute(
            "CREATE TABLE widgets (_id int PRIMARY KEY, name text, secret text)",
            "CREATE PUBLICATION $pub FOR TABLE widgets",
        )

        val node = openNode(tmp.resolve("log"), tmp.resolve("storage"))
        try {
            attachCdcWithReroute(
                node, tmp.resolve("cdc-log"), tmp.resolve("cdc-storage"),
                slot, pub, target = "gadgets", dropColumn = "secret",
            )
            awaitStreaming(node)

            commitTx("INSERT INTO widgets (_id, name, secret) VALUES (?, ?, ?)") { ps ->
                ps.setInt(1, 1); ps.setString(2, "anvil"); ps.setString(3, "classified")
            }

            // the row lands in the rerouted `gadgets` table (proving reroute), with `secret` absent
            // from the projection (proving the drop) — `name` survives
            val gadgets = await(
                done = { rows: List<Map<String, Any?>> -> rows.isNotEmpty() },
            ) { xtQuery(node, database = "cdc", sql = "SELECT * FROM gadgets") }

            assertEquals(1, gadgets.size)
            assertEquals("anvil", gadgets[0]["name"])
            assertFalse(gadgets[0].containsKey("secret"), "dropped column must not appear")
        } finally {
            node.close()
            dropSlot(slot)
        }
    }
}
