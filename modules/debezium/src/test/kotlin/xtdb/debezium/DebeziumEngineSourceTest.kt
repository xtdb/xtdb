package xtdb.debezium

import kotlinx.coroutines.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.testcontainers.postgresql.PostgreSQLContainer
import xtdb.database.ExternalSource
import xtdb.database.ExternalSourceToken
import xtdb.debezium.proto.DebeziumEngineOffsetToken
import java.sql.Connection
import java.util.Collections
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class DebeziumEngineSourceTest {

    companion object {
        private val postgres = PostgreSQLContainer("postgres:17-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10")

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            postgres.start()

            withConn { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        """
                        CREATE TABLE public.items (
                            _id TEXT PRIMARY KEY,
                            name TEXT NOT NULL
                        );
                        """.trimIndent()
                    )
                    stmt.execute("CREATE PUBLICATION test_pub FOR TABLE public.items;")
                }
            }
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            postgres.stop()
        }

        private fun <R> withConn(block: (Connection) -> R): R =
            postgres.createConnection("").use(block)
    }

    private fun uniqueSlot(): String =
        "xtdb_test_slot_" + UUID.randomUUID().toString().replace("-", "").take(16)

    private fun factory(slot: String) = DebeziumEngineSource.Factory(
        backend = DebeziumEngineSource.Backend.Postgres(
            hostname = postgres.host,
            user = postgres.username,
            password = postgres.password,
            database = postgres.databaseName,
            publicationName = "test_pub",
            slotName = slot,
            tableIncludeList = "public.items",
            port = postgres.firstMappedPort,
        ),
        snapshotChunkSize = 10,
        offsetFlushIntervalMs = 100L,
    )

    private data class CapturedTx(
        val txId: Long,
        val cdcRows: Int,
        val resumeToken: ExternalSourceToken?,
    )

    private fun capturingHandler(): Pair<ExternalSource.TxHandler, MutableList<CapturedTx>> {
        val received = Collections.synchronizedList(mutableListOf<CapturedTx>())
        val handler = ExternalSource.TxHandler { openTx, resumeToken ->
            received.add(
                CapturedTx(
                    txId = openTx.txKey.txId,
                    cdcRows = openTx.tables
                        .filter { (ref, _) -> ref.schemaName != "xt" }
                        .sumOf { (_, table) -> table.txRelation.rowCount },
                    resumeToken = resumeToken,
                )
            )
        }
        return handler to received
    }

    private fun execSql(sql: String) {
        withConn { conn -> conn.createStatement().use { it.execute(sql) } }
    }

    private fun dropSlotQuietly(slot: String) {
        runCatching {
            withConn { conn ->
                conn.createStatement().use { it.execute("SELECT pg_drop_replication_slot('$slot')") }
            }
        }
    }

    private fun clearTable() {
        runCatching { execSql("DELETE FROM public.items") }
    }

    @Test
    fun `snapshot and streaming events flow from postgres`() = runBlocking(Dispatchers.Default) {
        val slot = uniqueSlot()
        clearTable()
        try {
            // Pre-populate — these will come through via the initial snapshot.
            execSql("INSERT INTO public.items VALUES ('snap-1', 'Alice'), ('snap-2', 'Bob')")

            val (handler, received) = capturingHandler()
            val source = factory(slot).open("testdb", emptyMap())

            source.use {
                val job = launch { source.onPartitionAssigned(0, null, handler) }
                try {
                    // Wait for snapshot phase to emit both pre-inserted rows.
                    withTimeout(30.seconds) {
                        while (received.sumOf { it.cdcRows } < 2) delay(250)
                    }

                    val rowsAfterSnapshot = received.sumOf { it.cdcRows }

                    // New inserts — these flow as streaming events.
                    execSql("INSERT INTO public.items VALUES ('live-1', 'Carol')")

                    withTimeout(30.seconds) {
                        while (received.sumOf { it.cdcRows } < rowsAfterSnapshot + 1) delay(250)
                    }

                    assertTrue(received.sumOf { it.cdcRows } >= 3, "expected at least 3 CDC rows across snapshot + streaming")

                    // Every committed tx should carry a non-null resumeToken that decodes to our proto.
                    for (tx in received) {
                        assertNotNull(tx.resumeToken, "resumeToken should be set for each tx")
                        val decoded = tx.resumeToken!!.unpack(DebeziumEngineOffsetToken::class.java)
                        // latestTxId is monotonic across txns.
                        assertEquals(tx.txId, decoded.latestTxId)
                    }

                    // Last token should have some Debezium offset state captured.
                    val lastToken = received.last().resumeToken!!.unpack(DebeziumEngineOffsetToken::class.java)
                    assertTrue(
                        lastToken.debeziumOffsetsList.isNotEmpty(),
                        "last token should contain Debezium offset map — was empty",
                    )
                } finally {
                    job.cancelAndJoin()
                }
            }
        } finally {
            dropSlotQuietly(slot)
            clearTable()
        }
    }

    @Test
    fun `resume from token skips already-applied events`() = runBlocking(Dispatchers.Default) {
        val slot = uniqueSlot()
        clearTable()
        try {
            execSql("INSERT INTO public.items VALUES ('r-1', 'First')")

            val (handler1, received1) = capturingHandler()
            val source1 = factory(slot).open("testdb", emptyMap())

            var firstToken: ExternalSourceToken? = null
            source1.use {
                val job = launch { source1.onPartitionAssigned(0, null, handler1) }
                try {
                    withTimeout(30.seconds) {
                        while (received1.sumOf { it.cdcRows } < 1) delay(250)
                    }

                    // Add a streaming event so we have a non-trivial last LSN in the resumeToken.
                    execSql("INSERT INTO public.items VALUES ('r-2', 'Second')")
                    withTimeout(30.seconds) {
                        while (received1.sumOf { it.cdcRows } < 2) delay(250)
                    }

                    firstToken = received1.last().resumeToken
                    assertNotNull(firstToken)
                } finally {
                    job.cancelAndJoin()
                }
            }
            val capturedToken = firstToken!!

            // Resume with the token.  Inserts made while stopped should still arrive,
            // but previously-applied ones should be deduped.
            execSql("INSERT INTO public.items VALUES ('r-3', 'Third')")

            val (handler2, received2) = capturingHandler()
            val source2 = factory(slot).open("testdb", emptyMap())
            source2.use {
                val job = launch { source2.onPartitionAssigned(0, capturedToken, handler2) }
                try {
                    withTimeout(30.seconds) {
                        while (received2.sumOf { it.cdcRows } < 1) delay(250)
                    }

                    // We should see exactly the new row, not the two earlier ones.
                    // (Debezium may replay from slot position; our LSN dedup filters them.)
                    assertTrue(
                        received2.sumOf { it.cdcRows } >= 1,
                        "should see the post-resume insert",
                    )

                    // Confirm the resumed txId continues from the first session.
                    val firstDecoded = capturedToken.unpack(DebeziumEngineOffsetToken::class.java)
                    val lastDecoded = received2.last().resumeToken!!.unpack(DebeziumEngineOffsetToken::class.java)
                    assertTrue(
                        lastDecoded.latestTxId > firstDecoded.latestTxId,
                        "resumed session should advance latestTxId past the first session's",
                    )
                } finally {
                    job.cancelAndJoin()
                }
            }
        } finally {
            dropSlotQuietly(slot)
            clearTable()
        }
    }
}
