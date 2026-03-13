package xtdb.debezium

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Xtdb
import xtdb.api.log.Log.Record
import xtdb.api.log.SourceMessage
import java.time.Instant
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

class DebeziumProcessorTest {

    private fun putRecord(
        id: Int,
        name: String,
        op: String = "c",
        offset: Long = 0,
        table: String = "test",
    ): Record<SourceMessage> {
        val envelope = buildJsonObject {
            putJsonObject("payload") {
                put("op", op)
                putJsonObject("after") { put("_id", id); put("name", name) }
                put("before", JsonNull)
                putJsonObject("source") {
                    put("schema", "public")
                    put("table", table)
                    put("lsn", 100)
                }
            }
        }
        return Record(0, offset, Instant.now(), SourceMessage.Tx(envelope.toString().toByteArray()))
    }

    private fun deleteRecord(
        id: Int,
        offset: Long = 0,
        table: String = "test",
    ): Record<SourceMessage> {
        val envelope = buildJsonObject {
            putJsonObject("payload") {
                put("op", "d")
                put("after", JsonNull)
                putJsonObject("before") { put("_id", id) }
                putJsonObject("source") {
                    put("schema", "public")
                    put("table", table)
                    put("lsn", 100)
                }
            }
        }
        return Record(0, offset, Instant.now(), SourceMessage.Tx(envelope.toString().toByteArray()))
    }

    private fun xtQuery(node: Xtdb, sql: String): List<Map<String, Any?>> {
        return node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    val metadata = rs.metaData
                    val cols = (1..metadata.columnCount).map { metadata.getColumnName(it) }
                    buildList {
                        while (rs.next()) {
                            add(cols.associateWith { rs.getObject(it) })
                        }
                    }
                }
            }
        }
    }

    private fun rawRecord(payload: String, offset: Long = 0): Record<SourceMessage> =
        Record(0, offset, Instant.now(), SourceMessage.Tx(payload.toByteArray()))

    private fun awaitTxs(node: Xtdb, expected: Int, timeout: kotlin.time.Duration = 10.seconds) {
        val start = TimeSource.Monotonic.markNow()
        while (true) {
            val count = xtQuery(node, "SELECT count(*) AS cnt FROM xt.txs")[0]["cnt"] as Long
            if (count >= expected) return
            check(start.elapsedNow() < timeout) { "Timed out waiting for $expected txs (got $count)" }
            Thread.sleep(50)
        }
    }

    private fun dlqTxs(node: Xtdb): List<Map<String, Any?>> = xtQuery(
        node,
        """SELECT (user_metadata).error, (user_metadata).kafka_offset
           FROM xt.txs
           WHERE (user_metadata).source = 'debezium'
             AND (user_metadata).error IS NOT NULL
           ORDER BY _id"""
    )

    @Test
    fun `invalid JSON goes to DLQ`() = runTest {
        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)

            processor.processRecords(listOf(rawRecord("not json at all", offset = 42)))

            awaitTxs(node, 1)

            val dlq = dlqTxs(node)
            assertEquals(1, dlq.size)
            assertEquals(42L, (dlq[0]["kafka_offset"] as Number).toLong())
        }
    }

    @Test
    fun `JSON array goes to DLQ`() = runTest {
        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)

            processor.processRecords(listOf(rawRecord("[1, 2, 3]")))

            awaitTxs(node, 1)

            val dlq = dlqTxs(node)
            assertEquals(1, dlq.size)
        }
    }

    @Test
    fun `unknown op goes to DLQ`() = runTest {
        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)

            val envelope = buildJsonObject {
                putJsonObject("payload") {
                    put("op", "x")
                    putJsonObject("after") { put("_id", 1) }
                    putJsonObject("source") { put("schema", "public"); put("table", "t") }
                }
            }
            processor.processRecords(listOf(rawRecord(envelope.toString())))

            awaitTxs(node, 1)

            val dlq = dlqTxs(node)
            assertEquals(1, dlq.size)
            assertTrue((dlq[0]["error"] as String).contains("Unknown CDC op"))
        }
    }

    @Test
    fun `missing _id goes to DLQ`() = runTest {
        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)

            val envelope = buildJsonObject {
                putJsonObject("payload") {
                    put("op", "c")
                    putJsonObject("after") { put("name", "no id") }
                    putJsonObject("source") { put("schema", "public"); put("table", "t") }
                }
            }
            processor.processRecords(listOf(rawRecord(envelope.toString())))

            awaitTxs(node, 1)

            val dlq = dlqTxs(node)
            assertEquals(1, dlq.size)
            assertTrue((dlq[0]["error"] as String).contains("_id"))
        }
    }

    @Test
    fun `empty record list is a no-op`() = runTest {
        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)

            processor.processRecords(emptyList())

            val txs = xtQuery(node, "SELECT * FROM xt.txs")
            assertEquals(0, txs.size)
        }
    }

    @Test
    fun `valid records in batch still processed when others fail`() = runTest {
        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)

            val batch = listOf(
                putRecord(1, "Alice", offset = 0),
                rawRecord("not json", offset = 1),
                putRecord(2, "Bob", offset = 2),
            )

            processor.processRecords(batch)

            // 3 txs: Alice, DLQ for invalid, Bob
            awaitTxs(node, 3)

            // Alice and Bob should be ingested
            val rows = xtQuery(node, "SELECT _id, name FROM public.test ORDER BY _id")
            assertEquals(2, rows.size)
            assertEquals("Alice", rows[0]["name"])
            assertEquals("Bob", rows[1]["name"])

            // Invalid record should be in DLQ
            val dlq = dlqTxs(node)
            assertEquals(1, dlq.size)
            assertEquals(1L, (dlq[0]["kafka_offset"] as Number).toLong())
        }
    }

    @Test
    fun `node failure propagates out of processRecords`() = runTest {
        val node = Xtdb.openNode { server { port = 0 }; flightSql = null }
        val processor = DebeziumProcessor(node, "xtdb", node.allocator)

        // Close the node — submitTx will now fail
        node.close()

        val record = putRecord(1, "Alice")

        // Infrastructure error should propagate, NOT be caught by DLQ handler
        assertThrows<Exception> {
            processor.processRecords(listOf(record))
        }
    }

    @Test
    fun `batch of mixed ops processes all records`() = runTest {
        Xtdb.openNode { server { port = 0 }; flightSql = null }.use { node ->
            val processor = DebeziumProcessor(node, "xtdb", node.allocator)

            val batch = listOf(
                putRecord(1, "Alice", op = "c", offset = 0),
                putRecord(2, "Bob", op = "c", offset = 1),
                putRecord(1, "Alice Updated", op = "u", offset = 2),
                deleteRecord(2, offset = 3),
            )

            processor.processRecords(batch)

            awaitTxs(node, 4)

            val rows = xtQuery(
                node,
                """SELECT _id, name, _valid_from, _valid_to
                   FROM public.test
                   FOR ALL VALID_TIME
                   ORDER BY _id, _valid_from"""
            )

            // Alice: created then updated = 2 rows
            // Bob: created then deleted = 1 row with valid_to
            assertEquals(3, rows.size, "Expected 3 history rows")

            assertEquals(1L, (rows[0]["_id"] as Number).toLong())
            assertEquals("Alice", rows[0]["name"])

            assertEquals(1L, (rows[1]["_id"] as Number).toLong())
            assertEquals("Alice Updated", rows[1]["name"])

            assertEquals(2L, (rows[2]["_id"] as Number).toLong())
            assertEquals("Bob", rows[2]["name"])
            assertTrue(rows[2]["_valid_to"] != null)
        }
    }
}
