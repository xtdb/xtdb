package xtdb

import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import xtdb.error.Incorrect
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.ofType

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
    fun `getParameterSchema reports columns in $ N convention`() {
        insertData("CREATE TABLE foo (_id, name)")

        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id FROM foo WHERE _id = ? AND name = ?")
                stmt.prepare()

                val paramSchema = stmt.parameterSchema
                assertEquals(2, paramSchema.fields.size, "expected 2 parameter slots")
                assertEquals("\$0", paramSchema.fields[0].name)
                assertEquals("\$1", paramSchema.fields[1].name)
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

            assertEquals(
                listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "the buffered write is not yet committed"
            )

            conn.commit()
            assertEquals(
                listOf(mapOf("_id" to 0L), mapOf("_id" to 1L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "visible once committed"
            )
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

            assertEquals(
                listOf(mapOf("_id" to 0L)), conn.select("SELECT _id FROM foo ORDER BY _id"),
                "buffered until COMMIT"
            )

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
    fun `BEGIN READ WRITE WITH TIMEZONE is rejected`() {
        xtdb.connect().use { conn ->
            assertThrows(Incorrect::class.java) { conn.update("BEGIN READ WRITE WITH (TIMEZONE = 'America/New_York')") }
        }
    }

    @Test
    fun `BEGIN READ ONLY WITH SNAPSHOT_TOKEN is rejected`() {
        xtdb.connect().use { conn ->
            assertThrows(Incorrect::class.java) { conn.update("BEGIN READ ONLY WITH (SNAPSHOT_TOKEN = 'tok')") }
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
}
