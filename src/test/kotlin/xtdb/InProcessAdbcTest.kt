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
}
