package xtdb.postgres

import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class PgWireDriverTest : PostgresSourceTestBase() {

    @Test
    fun `an op's description lists its table's columns, snapshot and streaming alike`() = runTest(timeout = 60.seconds) {
        val slot = unique("described_slot")
        val pub = unique("described_pub")
        val table = unique("described")

        pgExecute(
            "CREATE TABLE $table (_id int PRIMARY KEY, name text, born date)",
            "INSERT INTO $table VALUES (1, 'alice', '1990-01-01')",
            "CREATE PUBLICATION $pub FOR TABLE $table",
        )

        val int4 = 23
        val text = 25
        val date = 1082
        val described = PgTable(
            "public", table,
            listOf(PgColumn("_id", int4), PgColumn("name", text), PgColumn("born", date)),
        )

        try {
            pgWireDriver(slot, pub).use { driver ->
                val slotLsn = driver.openSnapshot().use { snapshot ->
                    assertEquals(listOf(described), snapshot.batches().flatten().map { it.source }.toList())
                    snapshot.slotLsn
                }

                pgExecute(
                    "BEGIN",
                    "INSERT INTO $table VALUES (2, 'bob', NULL)",
                    "UPDATE $table SET name = 'carol' WHERE _id = 2",
                    "DELETE FROM $table WHERE _id = 1",
                    "COMMIT",
                )

                driver.openStream(slotLsn).use { stream ->
                    val tx = eventually(30.seconds) { assertNotNull(stream.poll()) }
                    assertEquals(listOf(described, described, described), tx.ops.map { it.source })
                }
            }
        } finally {
            dropSlot(slot)
        }
    }
}
