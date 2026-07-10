package xtdb.postgres

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import java.nio.file.Files
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class PostgresSourceToastTest : PostgresSourceTestBase() {

    private fun cdcIngestionError(node: Xtdb) =
        (node as Xtdb.XtdbInternal).dbCatalog["cdc"]?.ingestionError

    // ~10KB of non-repeating text, forced out-of-line via EXTERNAL storage, so `blob` is reliably
    // TOASTed and an UPDATE that doesn't touch it sends it as "unchanged" in the pgoutput message.
    private val bigBlob = (0 until 10_000).joinToString("") { (('a' + (it * 7 + it / 3) % 26)).toString() }

    private suspend fun updateLeavingToastUnchanged(replicaIdentityFull: Boolean, check: suspend (Xtdb, String) -> Unit) {
        val table = unique("pg"); val pub = unique("pub"); val slot = unique("slot")
        val dirs = List(4) { Files.createTempDirectory("pg-toast") }
        pgExecute(
            "CREATE TABLE $table (_id INT PRIMARY KEY, name TEXT, blob TEXT)",
            "ALTER TABLE $table ALTER COLUMN blob SET STORAGE EXTERNAL",
            *(if (replicaIdentityFull) arrayOf("ALTER TABLE $table REPLICA IDENTITY FULL") else emptyArray()),
            "CREATE PUBLICATION $pub FOR TABLE $table",
        )
        val node = openNode(dirs[0], dirs[1])
        try {
            attachCdc(node, database = "cdc", cdcLog = dirs[2], cdcStorage = dirs[3], slot = slot, pub = pub)
            awaitStreaming(node)
            flushBlock(node)
            commitTx("INSERT INTO $table (_id, name, blob) VALUES (1, 'before', ?)") { it.setString(1, bigBlob) }
            commitTx("UPDATE $table SET name = 'after' WHERE _id = 1") { }
            check(node, table)
        } finally {
            node.close()
            runCatching { dropSlot(slot) }
            dirs.forEach { it.toFile().deleteRecursively() }
        }
    }

    @Test
    fun `unchanged TOASTed column without REPLICA IDENTITY FULL halts the source`() = runTest(timeout = 5.minutes) {
        updateLeavingToastUnchanged(replicaIdentityFull = false) { node, _ ->
            val err = await(timeout = 30.seconds, done = { it != null }) { cdcIngestionError(node) }
            assertNotNull(err, "expected ingestion to halt on the unchanged TOASTed column")
            assertTrue(
                err!!.message!!.contains("REPLICA IDENTITY FULL"),
                "error should point at the REPLICA IDENTITY FULL remedy, got: ${err.message}",
            )
        }
    }

    @Test
    fun `REPLICA IDENTITY FULL lets an unchanged TOASTed column stream`() = runTest(timeout = 5.minutes) {
        updateLeavingToastUnchanged(replicaIdentityFull = true) { node, table ->
            val rows = await(
                timeout = 30.seconds,
                done = { r -> r.any { (it["name"] as String?) == "after" } },
            ) {
                runCatching { xtQuery(node, "cdc", "SELECT _id, name, blob FROM public.$table ORDER BY _id") }
                    .getOrDefault(emptyList())
            }
            assertNull(cdcIngestionError(node), "cdc ingestion should not have stopped with REPLICA IDENTITY FULL")
            assertEquals(1, rows.size)
            assertEquals("after", rows[0]["name"])
            assertEquals(bigBlob, rows[0]["blob"])
        }
    }
}
