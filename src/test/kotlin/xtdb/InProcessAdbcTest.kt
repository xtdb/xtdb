package xtdb

import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import xtdb.api.error.Incorrect
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.database.Database
import xtdb.database.encodeTimeBasisToken
import java.time.Instant
import java.util.UUID

class InProcessAdbcTest {

    private lateinit var xtdb: Xtdb
    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        xtdb = Xtdb.openNode {
            server { port = 0 }
            flightSql { port = 0 }
        }

        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
        xtdb.close()
    }

    private fun insertData(sql: String) {
        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery(sql)
                stmt.executeUpdate()
            }
        }
    }

    private fun queryRows(stmt: AdbcStatement): List<Map<*, *>> =
        stmt.executeQuery().reader.use { rdr ->
            assertTrue(rdr.loadNextBatch())
            Relation.fromRoot(al, rdr.vectorSchemaRoot).use { rel ->
                rel.toMaps(SNAKE_CASE_STRING)
            }
        }

    private fun bindLongParam(stmt: AdbcStatement, value: Long) {
        Relation(al, "\$0" ofType I64).use { rel ->
            rel.writeRow(mapOf("\$0" to value))

            rel.openAsRoot(al).use { params ->
                stmt.bind(params)
            }
        }
    }

    @Test
    fun `prepare and execute simple query`() {
        insertData("INSERT INTO foo RECORDS {_id: 1, n: 'one'}")

        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id, n FROM foo")
                stmt.prepare()

                assertEquals(listOf(mapOf("_id" to 1L, "n" to "one")), queryRows(stmt))
            }
        }
    }

    @Test
    fun `prepare with bound parameter`() {
        insertData("INSERT INTO foo RECORDS {_id: 1}, {_id: 2}, {_id: 3}")

        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id FROM foo WHERE _id = ?")
                stmt.prepare()
                bindLongParam(stmt, 2L)
                assertEquals(listOf(mapOf("_id" to 2L)), queryRows(stmt))
            }
        }
    }

    @Test
    fun `getParameterSchema reports unnamed positional params as empty strings`() {
        insertData("CREATE TABLE foo (_id, name)")

        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id FROM foo WHERE _id = ? AND name = ?")
                stmt.prepare()

                val paramSchema = stmt.parameterSchema
                assertEquals(2, paramSchema.fields.size, "expected 2 parameter slots")
                assertEquals("", paramSchema.fields[0].name)
                assertEquals("", paramSchema.fields[1].name)
                assertEquals(ArrowType.Null.INSTANCE, paramSchema.fields[0].type)
                assertEquals(ArrowType.Null.INSTANCE, paramSchema.fields[1].type)
            }
        }
    }

    @Test
    fun `prepared DML with bound parameters`() {
        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("INSERT INTO foo (_id) VALUES (?)")
                stmt.prepare()
                bindLongParam(stmt, 42L)
                stmt.executeUpdate()
            }

            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id FROM foo")
                assertEquals(listOf(mapOf("_id" to 42L)), queryRows(stmt))
            }
        }
    }

    @Test
    fun `bind state is sticky across executes`() {
        insertData("INSERT INTO foo RECORDS {_id: 1}, {_id: 2}, {_id: 3}")

        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id FROM foo WHERE _id = ?")
                stmt.prepare()
                bindLongParam(stmt, 2L)

                fun runOnce(): List<Map<*, *>> = queryRows(stmt)

                val expected = listOf(mapOf("_id" to 2L))
                assertEquals(expected, runOnce(), "first execute")
                assertEquals(expected, runOnce(), "second execute reuses the same bind")
            }
        }
    }

    @Test
    fun `executeQuery without prepare fails loud when parameters are bound`() {
        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id FROM foo WHERE _id = ?")
                Relation(al, "\$0" ofType I64).use { rel ->
                    rel.writeRow(mapOf("\$0" to 1L))
                    rel.openAsRoot(al).use { stmt.bind(it) }
                }

                val ex = assertThrows(Incorrect::class.java) {
                    stmt.executeQuery()
                }
                assertTrue(
                    ex.message?.contains("prepare") == true,
                    "expected prepare-required message, got: ${ex.message}"
                )
            }
        }
    }

    @Test
    fun `setSqlQuery clears prior bind`() {
        insertData("INSERT INTO foo RECORDS {_id: 1}")

        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id FROM foo WHERE _id = ?")
                stmt.prepare()
                bindLongParam(stmt, 1L)

                stmt.setSqlQuery("SELECT _id FROM foo")
                stmt.prepare()
                assertEquals(
                    listOf(mapOf("_id" to 1L)),
                    queryRows(stmt),
                    "stale bind from prior SQL must not leak into new SQL"
                )
            }
        }
    }

    private fun allRows(stmt: AdbcStatement): List<Map<*, *>> {
        val res = mutableListOf<Map<*, *>>()
        stmt.executeQuery().reader.use { rdr ->
            while (rdr.loadNextBatch())
                Relation.fromRoot(al, rdr.vectorSchemaRoot).use { res.addAll(it.toMaps(SNAKE_CASE_STRING)) }
        }
        return res
    }

    private fun Xtdb.Connection.select(sql: String) =
        createStatement().use { stmt -> stmt.setSqlQuery(sql); allRows(stmt) }

    private fun Xtdb.Connection.update(sql: String) =
        createStatement().use { stmt -> stmt.setSqlQuery(sql); stmt.executeUpdate() }

    @Test
    fun `commit and rollback are illegal in autocommit mode`() {
        xtdb.connect().use { conn ->
            assertThrows(Incorrect::class.java) { conn.commit() }
            assertThrows(Incorrect::class.java) { conn.rollback() }
        }
    }

    @Test
    fun `manual-mode writes are buffered until commit`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.setAutoCommit(false)
            conn.update("INSERT INTO foo RECORDS {_id: 1}")

            xtdb.connect().use { other ->
                assertEquals(
                    listOf(mapOf("_id" to 0L)), other.select("SELECT _id FROM foo ORDER BY _id"),
                    "the buffered write is not visible to another connection before commit"
                )
            }

            conn.commit()
            assertEquals(
                listOf(mapOf("_id" to 0L), mapOf("_id" to 1L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "visible once committed"
            )
        }
    }

    @Test
    fun `a query in a read-write transaction is rejected`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")

            assertThrows(Incorrect::class.java) { conn.select("SELECT _id FROM foo") }
        }
    }

    @Test
    fun `a query resolves a bare BEGIN to read-only, then DML is rejected`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.update("BEGIN")
            assertEquals(listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"))

            assertThrows(Incorrect::class.java) { conn.update("INSERT INTO foo RECORDS {_id: 1}") }
        }
    }

    @Test
    fun `a read-only transaction is isolated from a concurrent write`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.update("BEGIN READ ONLY")
            assertEquals(listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"))

            insertData("INSERT INTO foo RECORDS {_id: 1}")

            assertEquals(
                listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "the concurrent write is invisible within the begin-pinned snapshot"
            )

            conn.update("COMMIT")
            assertEquals(
                listOf(mapOf("_id" to 0L), mapOf("_id" to 1L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "visible once the snapshot is released"
            )
        }
    }

    @Test
    fun `manual-mode reads share a snapshot without an explicit BEGIN`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.setAutoCommit(false)
            assertEquals(listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"))

            insertData("INSERT INTO foo RECORDS {_id: 1}")

            assertEquals(
                listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "the first read lazily pinned the snapshot; the concurrent write stays invisible"
            )

            conn.rollback()
        }
    }

    @Test
    fun `current-time is pinned across a read-only transaction`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN READ ONLY")
            val first = conn.select("SELECT CURRENT_TIMESTAMP ts")
            val second = conn.select("SELECT CURRENT_TIMESTAMP ts")
            assertEquals(first, second, "current_timestamp is fixed at BEGIN, identical across reads in the tx")
            conn.update("COMMIT")
        }
    }

    @Test
    fun `READ ONLY WITH CLOCK_TIME pins the transaction wall-clock`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN READ ONLY WITH (CLOCK_TIME = TIMESTAMP '2021-07-01T00:00:00Z')")
            assertEquals(
                listOf(mapOf("in_2021" to true)),
                conn.select(
                    "SELECT (CURRENT_TIMESTAMP >= TIMESTAMP '2021-01-01T00:00:00Z'" +
                        " AND CURRENT_TIMESTAMP < TIMESTAMP '2022-01-01T00:00:00Z') in_2021"
                )
            )
            conn.update("COMMIT")
        }
    }

    @Test
    fun `COMMIT SYNC and COMMIT ASYNC via SQL`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN READ WRITE")
            conn.update("INSERT INTO foo RECORDS {_id: 'cs1'}")
            conn.update("COMMIT SYNC")
            assertEquals(
                listOf(mapOf("_id" to "cs1")),
                conn.select("SELECT _id FROM foo WHERE _id = 'cs1'"),
                "COMMIT SYNC commits and the row is immediately visible"
            )

            // the sync commit awaited the outcome, so SHOW reports the full detail
            val synced = conn.select("SHOW latest_submitted_tx").single()
            assertEquals(true, synced["committed"])
            assertNotNull(synced["system_time"], "a sync commit records the awaited system-time")
        }

        xtdb.connect().use { conn ->
            conn.update("BEGIN READ WRITE")
            conn.update("INSERT INTO foo RECORDS {_id: 'ca1'}")
            conn.update("COMMIT ASYNC")
            assertEquals(
                listOf(mapOf("_id" to "ca1")),
                conn.select("SELECT _id FROM foo WHERE _id = 'ca1'"),
                "COMMIT ASYNC commits and the row is visible after the async tx settles"
            )

            // the async commit didn't await, so SHOW carries only the txId — no awaited detail
            val asynced = conn.select("SHOW latest_submitted_tx").single()
            assertNull(asynced["system_time"], "an async commit records only the txId, not the awaited detail")
        }
    }

    @Test
    fun `READ ONLY WITH AWAIT_TOKEN is rejected`() {
        xtdb.connect().use { conn ->
            assertThrows(Incorrect::class.java) {
                conn.update("BEGIN READ ONLY WITH (AWAIT_TOKEN = 'whatever')")
            }
        }
    }

    @Test
    fun `SET and SHOW AWAIT_TOKEN round-trip`() {
        xtdb.connect().use { conn ->
            conn.update("SET AWAIT_TOKEN = 'abc'")
            assertEquals(listOf(mapOf("await_token" to "abc")), conn.select("SHOW AWAIT_TOKEN"))
        }
    }

    @Test
    fun `SET and SHOW a session parameter round-trip`() {
        xtdb.connect().use { conn ->
            conn.update("SET datestyle = 'ISO'")
            assertEquals(listOf(mapOf("datestyle" to "ISO")), conn.select("SHOW datestyle"))
        }
    }

    @Test
    fun `SHOW latest_submitted_tx reports this connection's own writes`() {
        xtdb.connect().use { conn ->
            assertEquals(
                emptyList<Map<*, *>>(), conn.select("SHOW latest_submitted_tx"),
                "no rows until this connection has submitted a tx"
            )

            conn.update("INSERT INTO foo RECORDS {_id: 1}")

            val committed = conn.select("SHOW latest_submitted_tx").single()
            assertEquals(0L, committed["tx_id"])
            assertEquals(true, committed["committed"])
            assertNull(committed["error"])
            assertNotNull(committed["system_time"])
            assertNotNull(committed["await_token"])

            // a tx that aborts at index time is still recorded — committed=false with the error populated.
            // This exercises writing a Throwable into the transit-typed error column.
            runCatching { conn.update("ASSERT FALSE") }

            val aborted = conn.select("SHOW latest_submitted_tx").single()
            assertEquals(1L, aborted["tx_id"])
            assertEquals(false, aborted["committed"])
            assertNotNull(aborted["error"], "the aborted tx's error surfaces through the transit column")
        }
    }

    @Test
    fun `SET SESSION CHARACTERISTICS READ ONLY makes a bare BEGIN read-only`() {
        xtdb.connect().use { conn ->
            conn.update("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            conn.update("BEGIN")
            assertThrows(Incorrect::class.java) { conn.update("INSERT INTO foo RECORDS {_id: 1}") }
            conn.update("ROLLBACK")
        }
    }

    @Test
    fun `SET TIME ZONE is reflected by SHOW timezone`() {
        xtdb.connect().use { conn ->
            conn.update("SET TIME ZONE 'America/New_York'")
            assertEquals(listOf(mapOf("timezone" to "America/New_York")), conn.select("SHOW timezone"))
        }
    }

    @Test
    fun `a mid-transaction SET TIME ZONE is discarded on ROLLBACK and kept on COMMIT`() {
        xtdb.connect().use { conn ->
            conn.update("SET TIME ZONE 'UTC'")

            conn.update("BEGIN")
            conn.update("SET TIME ZONE 'America/New_York'")
            assertEquals(listOf(mapOf("timezone" to "America/New_York")), conn.select("SHOW timezone"))
            conn.update("ROLLBACK")
            assertEquals(
                listOf(mapOf("timezone" to "UTC")), conn.select("SHOW timezone"),
                "ROLLBACK reverts the mid-transaction SET TIME ZONE"
            )

            conn.update("BEGIN")
            conn.update("SET TIME ZONE 'America/New_York'")
            conn.update("COMMIT")
            assertEquals(
                listOf(mapOf("timezone" to "America/New_York")), conn.select("SHOW timezone"),
                "COMMIT keeps it"
            )
        }
    }

    @Test
    fun `SET TIME ZONE is rejected in a write transaction with buffered writes`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            assertThrows(Incorrect::class.java) { conn.update("SET TIME ZONE 'UTC'") }
            conn.update("ROLLBACK")
        }
    }

    @Test
    fun `SET TRANSACTION and SET ROLE are accepted no-ops`() {
        xtdb.connect().use { conn ->
            conn.update("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            conn.update("SET ROLE some_role")
        }
    }

    @Test
    fun `rollback discards buffered writes`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.setAutoCommit(false)
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            conn.rollback()

            assertEquals(listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"))
        }
    }

    @Test
    fun `switching to autocommit commits the open transaction`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.setAutoCommit(false)
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            conn.setAutoCommit(true)

            assertEquals(
                listOf(mapOf("_id" to 0L), mapOf("_id" to 1L)), conn.select("SELECT _id FROM foo ORDER BY _id")
            )
        }
    }

    @Test
    fun `consecutive prepared inserts coalesce and every row lands on commit`() {
        xtdb.connect().use { conn ->
            conn.setAutoCommit(false)
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("INSERT INTO foo (_id) VALUES (?)")
                stmt.prepare()
                for (id in 1L..5L) {
                    bindLongParam(stmt, id)
                    stmt.executeUpdate()
                }
            }
            conn.commit()

            assertEquals(
                (1L..5L).map { mapOf("_id" to it) },
                conn.select("SELECT _id FROM foo ORDER BY _id"),
                "every row of the coalesced run lands as a distinct insert"
            )
        }
    }

    @Test
    fun `a different statement seals the run, then a fresh run resumes`() {
        xtdb.connect().use { conn ->
            conn.setAutoCommit(false)

            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("INSERT INTO foo (_id) VALUES (?)")
                stmt.prepare()
                bindLongParam(stmt, 1L); stmt.executeUpdate()
                bindLongParam(stmt, 2L); stmt.executeUpdate()
            }

            conn.update("INSERT INTO bar RECORDS {_id: 10}")

            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("INSERT INTO foo (_id) VALUES (?)")
                stmt.prepare()
                bindLongParam(stmt, 3L); stmt.executeUpdate()
            }

            conn.commit()

            assertEquals((1L..3L).map { mapOf("_id" to it) }, conn.select("SELECT _id FROM foo ORDER BY _id"))
            assertEquals(listOf(mapOf("_id" to 10L)), conn.select("SELECT _id FROM bar"))
        }
    }

    @Test
    fun `rollback discards a buffered coalescing run`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.setAutoCommit(false)
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("INSERT INTO foo (_id) VALUES (?)")
                stmt.prepare()
                for (id in 1L..3L) {
                    bindLongParam(stmt, id)
                    stmt.executeUpdate()
                }
            }
            conn.rollback()

            assertEquals(
                listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "the buffered run is discarded; the run relation is freed (tearDown's allocator close would fail on a leak)"
            )
        }
    }

    @Test
    fun `SQL BEGIN COMMIT buffers then lands the writes`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.update("BEGIN")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")

            xtdb.connect().use { other ->
                assertEquals(
                    listOf(mapOf("_id" to 0L)), other.select("SELECT _id FROM foo ORDER BY _id"),
                    "buffered until COMMIT — not visible to another connection"
                )
            }

            conn.update("COMMIT")
            assertEquals(
                listOf(mapOf("_id" to 0L), mapOf("_id" to 1L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "visible once committed"
            )
        }
    }

    @Test
    fun `SQL ROLLBACK discards the buffered writes`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.update("BEGIN")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            conn.update("ROLLBACK")

            assertEquals(
                listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "the buffered write is discarded"
            )
        }
    }

    @Test
    fun `SQL BEGIN while a transaction is open is rejected`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN")
            assertThrows(Incorrect::class.java) { conn.update("BEGIN") }
        }
    }

    @Test
    fun `submitTx and executeTx are rejected while a transaction is open`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN")

            // the autonomous helpers can't be mixed with an explicit tx - they'd fire an independent
            // transaction, ignoring the buffered one
            assertThrows(Incorrect::class.java) { conn.submitTx(emptyList()) }
            assertThrows(Incorrect::class.java) { conn.executeTx(emptyList()) }
        }
    }

    @Test
    fun `executeUpdate rejects multi-statement input`() {
        xtdb.connect().use { conn ->
            assertThrows(Incorrect::class.java) { conn.update("BEGIN; COMMIT") }
        }
    }

    @Test
    fun `closing the connection discards an open transaction`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.update("BEGIN")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            // no COMMIT/ROLLBACK — the connection closes (via use) with the write still buffered
        }

        xtdb.connect().use { conn ->
            assertEquals(
                listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "an uncommitted tx is dropped on close, not submitted (and its buffer freed — node close would fail on a leak)"
            )
        }
    }

    @Test
    fun `BEGIN then COMMIT with no writes is a no-op`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.update("BEGIN")
            conn.update("COMMIT")

            assertEquals(
                listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "an unresolved tx commits as a no-op — nothing submitted"
            )
        }
    }

    @Test
    fun `BEGIN READ WRITE buffers and commits`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        xtdb.connect().use { conn ->
            conn.update("BEGIN READ WRITE")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            conn.update("COMMIT")

            assertEquals(
                listOf(mapOf("_id" to 0L), mapOf("_id" to 1L)), conn.select("SELECT _id FROM foo ORDER BY _id")
            )
        }
    }

    @Test
    fun `BEGIN READ ONLY rejects a write`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN READ ONLY")
            assertThrows(Incorrect::class.java) { conn.update("INSERT INTO foo RECORDS {_id: 1}") }
        }
    }

    @Test
    fun `BEGIN READ WRITE WITH SYSTEM_TIME applies the requested system-time`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN READ WRITE WITH (SYSTEM_TIME TIMESTAMP '2021-08-03T00:00:00')")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            conn.update("COMMIT")

            // connection defaultTz is UTC, so a zoneless TIMESTAMP literal anchors there
            assertEquals(
                listOf(mapOf("_system_from" to "2021-08-03T00:00Z[UTC]")),
                conn.select("SELECT _system_from FROM foo").map { row -> mapOf("_system_from" to row["_system_from"].toString()) },
                "the row's system-from is the requested system-time"
            )
        }
    }

    @Test
    fun `BEGIN READ WRITE WITH METADATA round-trips`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN READ WRITE WITH (METADATA = {source: 'mobile-app'})")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            conn.update("COMMIT")

            // the metadata is carried into the commit options; assert the tx committed and the row landed
            assertEquals(
                listOf(mapOf("_id" to 1L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "a READ WRITE tx carrying user-metadata commits its writes"
            )
        }
    }

    @Test
    fun `BEGIN READ WRITE WITH ASYNC commits and the row lands`() {
        xtdb.connect().use { conn ->
            conn.update("BEGIN READ WRITE WITH (ASYNC = TRUE)")
            conn.update("INSERT INTO foo RECORDS {_id: 1}")
            conn.update("COMMIT")

            assertEquals(
                listOf(mapOf("_id" to 1L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "an async commit still lands the row (the read awaits this connection's own write)"
            )
        }
    }

    @Test
    fun `BEGIN WITH TIMEZONE sets the tz for the transaction and reverts after`() {
        xtdb.connect().use { conn ->
            conn.update("SET TIME ZONE 'Asia/Tokyo'")

            conn.update("BEGIN READ WRITE WITH (TIMEZONE = 'America/New_York')")
            conn.update("INSERT INTO foo RECORDS {_id: 1, ts: (TIMESTAMP '2020-01-01'::timestamptz)}")
            conn.update("COMMIT")

            assertEquals(
                listOf(mapOf("_id" to 1L, "ts" to "2020-01-01T00:00-05:00[America/New_York]")),
                conn.select("SELECT _id, ts FROM foo").map { mapOf("_id" to it["_id"], "ts" to it["ts"].toString()) },
                "the DML anchors its zoneless timestamptz in the tx zone, not the session's Asia/Tokyo"
            )
            assertEquals(listOf(mapOf("timezone" to "Asia/Tokyo")), conn.select("SHOW timezone"), "reverts after COMMIT")

            conn.update("BEGIN READ ONLY WITH (TIMEZONE = 'America/New_York')")
            assertEquals(listOf(mapOf("timezone" to "America/New_York")), conn.select("SHOW timezone"))
            assertEquals(
                listOf(mapOf("ts" to "2020-01-01T00:00-05:00[America/New_York]")),
                conn.select("SELECT TIMESTAMP '2020-01-01'::timestamptz ts").map { mapOf("ts" to it["ts"].toString()) }
            )
            conn.update("COMMIT")
            assertEquals(listOf(mapOf("timezone" to "Asia/Tokyo")), conn.select("SHOW timezone"), "reverts after COMMIT")
        }
    }

    @Test
    fun `READ ONLY WITH SNAPSHOT_TOKEN bounds reads to the given snapshot`() {
        insertData("INSERT INTO foo RECORDS {_id: 0}")

        // a past snapshot bound — earlier than the row's commit, so the explicit token must decode, apply, and
        // bound the read below it: the row is excluded. (Avoid the epoch: it collides with the encoding's
        // "no completed tx" sentinel and decodes to null; a future token would be rejected as unindexed.)
        val token = mapOf("xtdb" to listOf<Instant?>(Instant.parse("2020-01-01T00:00:00Z"))).encodeTimeBasisToken()

        xtdb.connect().use { conn ->
            conn.update("BEGIN READ ONLY WITH (SNAPSHOT_TOKEN = '$token')")
            assertEquals(
                emptyList<Map<*, *>>(), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "as of the epoch the row's commit is in the future, so nothing is visible"
            )
            conn.update("COMMIT")
        }
    }

    @Test
    fun `empty READ WRITE commit still records a transaction`() {
        xtdb.connect().use { conn ->
            val before = conn.select("SELECT count(*) AS c FROM xt.txs").single()["c"] as Long
            conn.update("BEGIN READ WRITE")
            conn.update("COMMIT")
            val after = conn.select("SELECT count(*) AS c FROM xt.txs").single()["c"] as Long

            assertEquals(before + 1, after, "an explicit write tx commits even when empty")
        }
    }

    @Test
    fun `a literal UUID _id round-trips through static expansion`() {
        val id = "7f8e9d6c-5b4a-4c2d-9e0f-a9b8c7d6e5f4"
        insertData("INSERT INTO foo RECORDS {_id: UUID '$id', n: 1}")

        xtdb.connect().use { conn ->
            assertEquals(
                listOf(mapOf("_id" to UUID.fromString(id), "n" to 1L)),
                conn.select("SELECT _id, n FROM foo"),
                "a literal UUID id is coerced via PutDocs, not rejected as raw-SQL 'Invalid ID type: [B'"
            )
        }
    }

    @Test
    fun `a literal UUID _id in PATCH RECORDS round-trips`() {
        val id = "7f8e9d6c-5b4a-4c2d-9e0f-a9b8c7d6e5f4"
        insertData("INSERT INTO foo RECORDS {_id: UUID '$id', n: 1}")
        insertData("PATCH INTO foo RECORDS {_id: UUID '$id', n: 2}")

        xtdb.connect().use { conn ->
            assertEquals(
                listOf(mapOf("_id" to UUID.fromString(id), "n" to 2L)),
                conn.select("SELECT _id, n FROM foo")
            )
        }
    }

    @Test
    fun `INSERT RECORDS without _id surfaces the static missing-id error`() {
        xtdb.connect().use { conn ->
            val ex = assertThrows(Incorrect::class.java) { conn.update("INSERT INTO foo RECORDS {n: 1}") }
            assertTrue(ex.message?.contains("_id") == true, "expected a missing-_id message, got: ${ex.message}")
        }
    }

    @Test
    fun `a non-expandable DELETE falls back to the raw Sql op`() {
        insertData("INSERT INTO foo RECORDS {_id: 1}, {_id: 2}")

        xtdb.connect().use { conn ->
            conn.update("DELETE FROM foo WHERE _id = 1")
            assertEquals(
                listOf(mapOf("_id" to 2L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "DELETE isn't statically expandable — it submits the raw Sql op, expanded at index time"
            )
        }
    }

    // -- getObjects / getTableSchema across catalogs (databases) --

    private fun Any?.asList() = this as List<*>
    private fun Any?.asMap() = this as Map<*, *>

    private fun VectorSchemaRoot.dbSchemasOf(catalog: String): List<Map<*, *>> {
        val row = (0 until rowCount).first { getVector("catalog_name").getObject(it).toString() == catalog }
        return (getVector("catalog_db_schemas") as ListVector).getObject(row).asList().map { it.asMap() }
    }

    private fun VectorSchemaRoot.schemaNames(catalog: String): Set<String> =
        dbSchemasOf(catalog).map { it["db_schema_name"].toString() }.toSet()

    private fun VectorSchemaRoot.tablesOf(catalog: String, schema: String): Set<String> =
        dbSchemasOf(catalog).first { it["db_schema_name"].toString() == schema }["db_schema_tables"]
            .asList().map { it.asMap()["table_name"].toString() }.toSet()

    private fun VectorSchemaRoot.publicTables(catalog: String) = tablesOf(catalog, "public")

    private fun VectorSchemaRoot.columnsOf(catalog: String, schema: String, table: String): Set<String> {
        val tables = dbSchemasOf(catalog).first { it["db_schema_name"].toString() == schema }["db_schema_tables"].asList()
        return tables.first { it.asMap()["table_name"].toString() == table }.asMap()["table_columns"]
            .asList().map { it.asMap()["column_name"].toString() }.toSet()
    }

    @Test
    fun `getObjects expands every catalog, not just the connected one`() {
        insertData("INSERT INTO foo RECORDS {_id: 1}")
        xtdb.connect().use { it.attachDb("new_db", Database.Config()) }
        xtdb.connect("new_db").use { it.update("INSERT INTO bar RECORDS {_id: 2}") }

        xtdb.connect().use { conn ->
            conn.getObjects(GetObjectsDepth.ALL, null, null, null, null, null).use { rdr ->
                assertTrue(rdr.loadNextBatch())
                val root = rdr.vectorSchemaRoot

                val catalogs = (0 until root.rowCount)
                    .map { root.getVector("catalog_name").getObject(it).toString() }.toSet()
                assertEquals(setOf("new_db", "xtdb"), catalogs, "every catalog is listed")

                assertTrue("foo" in root.publicTables("xtdb"))
                assertTrue("bar" in root.publicTables("new_db"),
                    "a catalog other than the connection's own is expanded with its own tables")
                assertTrue("bar" !in root.publicTables("xtdb"))
                assertTrue("foo" !in root.publicTables("new_db"))
            }
        }
    }

    @Test
    fun `getObjects catalogPattern filters catalogs via SQL LIKE`() {
        xtdb.connect().use { it.attachDb("new_db", Database.Config()) }

        xtdb.connect().use { conn ->
            conn.getObjects(GetObjectsDepth.CATALOGS, "new%", null, null, null, null).use { rdr ->
                assertTrue(rdr.loadNextBatch())
                val root = rdr.vectorSchemaRoot
                val catalogs = (0 until root.rowCount)
                    .map { root.getVector("catalog_name").getObject(it).toString() }.toSet()
                assertEquals(setOf("new_db"), catalogs, "LIKE 'new%' matches new_db, not xtdb")
            }
        }
    }

    @Test
    fun `getObjects is not gated by the connection's transaction access-mode`() {
        xtdb.connect().use { conn ->
            conn.beginWriteOnly()
            conn.getObjects(GetObjectsDepth.CATALOGS, null, null, null, null, null).use { rdr ->
                assertTrue(rdr.loadNextBatch())
                val root = rdr.vectorSchemaRoot
                assertEquals(setOf("xtdb"), (0 until root.rowCount)
                    .map { root.getVector("catalog_name").getObject(it).toString() }.toSet())
            }
        }
    }

    @Test
    fun `getObjects tolerates a catalogPattern that isn't a valid SQL string literal`() {
        // a newline can't appear in a raw SQL string literal; the pattern is bound as a parameter, so this
        // returns no match rather than throwing a parse error
        xtdb.connect().use { conn ->
            conn.getObjects(GetObjectsDepth.CATALOGS, "no\nsuch", null, null, null, null).use { rdr ->
                assertTrue(rdr.loadNextBatch())
                assertEquals(0, rdr.vectorSchemaRoot.rowCount)
            }
        }
    }

    @Test
    fun `getTableSchema resolves a table in another catalog`() {
        xtdb.connect().use { it.attachDb("new_db", Database.Config()) }
        xtdb.connect("new_db").use { it.update("INSERT INTO bar RECORDS {_id: 2, b: 'x'}") }

        xtdb.connect().use { conn ->
            val schema = conn.getTableSchema("new_db", "public", "bar")
            assertEquals(setOf("_id", "b"), schema.fields.map { it.name }.toSet(),
                "resolves against the named catalog, not the connection's own db")
        }
    }

    @Test
    fun `getObjects dbSchemaPattern filters schemas via SQL LIKE`() {
        insertData("INSERT INTO foo RECORDS {_id: 1}")
        insertData("INSERT INTO reporting.bar RECORDS {_id: 1}")

        xtdb.connect().use { conn ->
            conn.getObjects(GetObjectsDepth.TABLES, null, "reporting", null, null, null).use { rdr ->
                assertTrue(rdr.loadNextBatch())
                val root = rdr.vectorSchemaRoot
                assertEquals(setOf("reporting"), root.schemaNames("xtdb"))
                assertEquals(setOf("bar"), root.tablesOf("xtdb", "reporting"))
            }
        }
    }

    @Test
    fun `getObjects tableNamePattern filters tables via SQL LIKE`() {
        insertData("INSERT INTO foo RECORDS {_id: 1}")
        insertData("INSERT INTO bar RECORDS {_id: 1}")

        xtdb.connect().use { conn ->
            conn.getObjects(GetObjectsDepth.TABLES, null, null, "foo", null, null).use { rdr ->
                assertTrue(rdr.loadNextBatch())
                assertEquals(setOf("foo"), rdr.vectorSchemaRoot.tablesOf("xtdb", "public"))
            }
        }
    }

    @Test
    fun `getObjects columnNamePattern filters columns via SQL LIKE`() {
        insertData("INSERT INTO foo RECORDS {_id: 1, name: 'a', age: 2}")

        xtdb.connect().use { conn ->
            conn.getObjects(GetObjectsDepth.ALL, null, null, "foo", null, "name").use { rdr ->
                assertTrue(rdr.loadNextBatch())
                assertEquals(setOf("name"), rdr.vectorSchemaRoot.columnsOf("xtdb", "public", "foo"))
            }
        }
    }

    @Test
    fun `getObjects tolerates schema table column patterns that aren't valid SQL string literals`() {
        insertData("INSERT INTO foo RECORDS {_id: 1, name: 'a'}")

        // a newline can't appear in a raw SQL string literal; the patterns are bound as parameters, so these
        // match nothing rather than throwing a parse error — cf. the catalogPattern case above
        xtdb.connect().use { conn ->
            conn.getObjects(GetObjectsDepth.ALL, null, "n\no", "n\no", null, "n\no").use { rdr ->
                assertTrue(rdr.loadNextBatch())
                assertEquals(emptySet<String>(), rdr.vectorSchemaRoot.schemaNames("xtdb"))
            }
        }
    }
}
