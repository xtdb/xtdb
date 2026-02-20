package xtdb

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.flight.CallOption
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.FlightInfo
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import java.time.ZonedDateTime
import java.util.*

class AdbcTest {

    private lateinit var xtdb: Xtdb

    private lateinit var al: BufferAllocator
    private lateinit var db: AdbcDatabase
    private lateinit var conn: AdbcConnection
    private lateinit var fsqlClient: FlightSqlClient

    private val emptyCallOpts = arrayOf<CallOption>()

    @BeforeEach
    fun setUp() {
        xtdb = Xtdb.openNode {
            server { port = 0 }
            flightSql { port = 0 }
        }

        val flightPort = xtdb.flightSqlPort

        al = RootAllocator()
        db = FlightSqlDriver(al).open(mapOf("uri" to "grpc+tcp://127.0.0.1:${flightPort}"))
        conn = db.connect()

        val flightClient = FlightClient.builder(al, Location.forGrpcInsecure("127.0.0.1", flightPort)).build()
        fsqlClient = FlightSqlClient(flightClient)
    }

    @AfterEach
    fun tearDown() {
        fsqlClient.close()
        conn.close()
        db.close()
        al.close()
        xtdb.close()
    }

    private fun FlightInfo.readRows(): List<Map<*, *>> {
        val ticket = endpoints.first().ticket
        fsqlClient.getStream(ticket, *emptyCallOpts).use { stream ->
            val root = stream.root
            Relation.fromRoot(al, root).use { rel ->
                val rows = mutableListOf<Map<*, *>>()
                while (stream.next()) {
                    rel.loadFromArrow(root)
                    rows.addAll(rel.toMaps(SNAKE_CASE_STRING))
                }
                return rows
            }
        }
    }

    private fun insertData(sql: String) {
        conn.createStatement().use { stmt ->
            stmt.setSqlQuery(sql)
            stmt.executeUpdate()
        }
    }

    // -- Basic round-trip --

    @Test
    fun `test simple round-trip`() {
        insertData("""
            INSERT INTO foo RECORDS
            {
              _id: UUID 'b82ae7b2-13cf-4828-858d-cd992fec9aa7',
              name: 'foo',
              created_at: TIMESTAMP '2020-01-01T12:34:00Z'
            }""".trimIndent())

        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT * FROM foo")
            stmt.executeQuery().reader.use { rdr ->
                val root = rdr.vectorSchemaRoot
                assertTrue(rdr.loadNextBatch())

                Relation.fromRoot(al, root).use { rel ->
                    assertEquals(
                        listOf(
                            mapOf(
                                "_id" to UUID.fromString("b82ae7b2-13cf-4828-858d-cd992fec9aa7"),
                                "name" to "foo",
                                "created_at" to ZonedDateTime.parse("2020-01-01T12:34:00Z")
                            )
                        ),
                        rel.toMaps(SNAKE_CASE_STRING)
                    )
                }

                assertFalse(rdr.loadNextBatch())
            }
        }
    }

    // -- FlightSQL metadata (via FlightSqlClient) --

    @Test
    fun `test FlightSQL getTableTypes`() {
        val rows = fsqlClient.getTableTypes(*emptyCallOpts).readRows()
        assertEquals(1, rows.size)
        assertEquals("TABLE", rows[0]["table_type"])
    }

    @Test
    fun `test FlightSQL getCatalogs`() {
        val rows = fsqlClient.getCatalogs(*emptyCallOpts).readRows()
        assertTrue(rows.isNotEmpty(), "Expected at least one catalog")
        assertEquals("xtdb", rows[0]["catalog_name"])
    }

    @Test
    fun `test FlightSQL getSchemas`() {
        insertData("INSERT INTO schema_test (_id) VALUES (1)")

        val rows = fsqlClient.getSchemas(null, null, *emptyCallOpts).readRows()
        assertTrue(rows.isNotEmpty(), "Expected at least one schema")

        val schemaNames = rows.map { it["db_schema_name"] }
        assertTrue("public" in schemaNames, "Expected 'public' schema in $schemaNames")
    }

    @Test
    fun `test FlightSQL getTables`() {
        insertData("INSERT INTO tables_test (_id) VALUES (1)")

        val rows = fsqlClient.getTables(null, null, null, null, false, *emptyCallOpts).readRows()
        assertTrue(rows.isNotEmpty(), "Expected at least one table")

        val tableNames = rows.map { it["table_name"] }
        assertTrue("tables_test" in tableNames, "Expected 'tables_test' in $tableNames")
    }

    @Test
    fun `test FlightSQL getSqlInfo`() {
        val rows = fsqlClient.getSqlInfo(intArrayOf(), *emptyCallOpts).readRows()
        assertTrue(rows.isNotEmpty(), "Expected at least one info row")
    }

    // -- ADBC metadata (through FlightSQL ADBC client) --

    // getInfo via ADBC client hits a bug in GetInfoMetadataReader.processRootFromStream
    // (allocates on the wrong root). Tested at the FlightSQL level via `test FlightSQL getSqlInfo` instead.

    @Test
    fun `test ADBC getObjects at CATALOGS depth`() {
        conn.getObjects(GetObjectsDepth.CATALOGS, null, null, null, null, null).use { rdr ->
            assertTrue(rdr.loadNextBatch())
            val root = rdr.vectorSchemaRoot
            assertEquals(1, root.rowCount)
            assertEquals("xtdb", root.getVector("catalog_name").getObject(0).toString())
            assertFalse(rdr.loadNextBatch())
        }
    }

    @Test
    fun `test ADBC getObjects at TABLES depth`() {
        insertData("INSERT INTO obj_test (_id, v) VALUES (1, 'x')")

        conn.getObjects(GetObjectsDepth.TABLES, null, null, null, null, null).use { rdr ->
            assertTrue(rdr.loadNextBatch())
            assertTrue(rdr.vectorSchemaRoot.rowCount > 0, "Expected catalog rows with table data")
        }
    }

    // -- Error handling --

    @Test
    fun `test DML via query path returns INVALID_ARGUMENT`() {
        val ex = assertThrows(org.apache.arrow.flight.FlightRuntimeException::class.java) {
            fsqlClient.execute("INSERT INTO users (_id, name) VALUES ('jms', 'James')", *emptyCallOpts)
        }
        assertEquals(org.apache.arrow.flight.FlightStatusCode.INVALID_ARGUMENT, ex.status().code())
        assertTrue(ex.message!!.contains("executeUpdate"), "Error should mention executeUpdate: ${ex.message}")
    }
}
