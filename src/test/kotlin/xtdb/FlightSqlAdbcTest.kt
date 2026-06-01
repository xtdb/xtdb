package xtdb

import com.google.protobuf.Any as ProtoAny
import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.flight.AsyncPutListener
import org.apache.arrow.flight.CallOption
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.FlightDescriptor
import org.apache.arrow.flight.FlightInfo
import org.apache.arrow.flight.FlightRuntimeException
import org.apache.arrow.flight.GetSessionOptionsRequest
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.NoOpSessionOptionValueVisitor
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.flight.sql.FlightSqlClient.ExecuteIngestOptions
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.Text
import java.io.ByteArrayInputStream
import java.nio.channels.Channels
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import java.time.ZonedDateTime
import java.util.*

class FlightSqlAdbcTest {

    private lateinit var xtdb: Xtdb

    private lateinit var al: BufferAllocator
    private lateinit var db: AdbcDatabase
    private lateinit var conn: AdbcConnection
    private lateinit var flightClient: FlightClient
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

        flightClient = FlightClient.builder(al, Location.forGrpcInsecure("127.0.0.1", flightPort)).build()
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

    @Test
    fun `test FlightSQL getSessionOptions returns catalog and schema`() {
        val result = fsqlClient.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)

        val asString = object : NoOpSessionOptionValueVisitor<String?>() {
            override fun visit(value: String) = value
        }
        val opts = result.sessionOptions
        assertEquals("xtdb", opts["catalog"]?.acceptVisitor(asString))
        assertEquals("public", opts["schema"]?.acceptVisitor(asString))
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

    // -- Bulk ingest --

    private fun defaultIngestTdo(): TableDefinitionOptions =
        TableDefinitionOptions.newBuilder()
            .setIfNotExist(TableNotExistOption.TABLE_NOT_EXIST_OPTION_CREATE)
            .setIfExists(TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
            .build()

    private fun ingestOpts(
        table: String,
        catalog: String? = null,
        schema: String? = null,
        tdo: TableDefinitionOptions = defaultIngestTdo(),
    ) = ExecuteIngestOptions(table, tdo, catalog, schema, null)

    private val usersSchema = Schema(listOf(
        Field("_id", FieldType.notNullable(Types.MinorType.BIGINT.type), null),
        Field("n", FieldType.notNullable(Types.MinorType.BIGINT.type), null),
    ))

    private fun <R> usersRoot(rows: List<Pair<Long, Long>>, block: (VectorSchemaRoot) -> R): R =
        VectorSchemaRoot.create(usersSchema, al).use { root ->
            val idVec = root.getVector("_id") as BigIntVector
            val nVec = root.getVector("n") as BigIntVector
            rows.forEachIndexed { i, (id, n) -> idVec.setSafe(i, id); nVec.setSafe(i, n) }
            root.rowCount = rows.size
            block(root)
        }

    private fun assertUsersTableContains(table: String, expected: List<Map<String, Long>>) {
        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT _id, n FROM $table ORDER BY _id")
            stmt.executeQuery().reader.use { rdr ->
                assertTrue(rdr.loadNextBatch())
                Relation.fromRoot(al, rdr.vectorSchemaRoot).use { rel ->
                    assertEquals(expected, rel.toMaps(SNAKE_CASE_STRING))
                }
            }
        }
    }

    @Test
    fun `test FlightSQL bulk ingest via executeIngest`() {
        usersRoot(listOf(1L to 10L, 2L to 20L)) { root ->
            fsqlClient.executeIngest(root, ingestOpts("users"), *emptyCallOpts)
        }
        assertUsersTableContains("users", listOf(
            mapOf("_id" to 1L, "n" to 10L),
            mapOf("_id" to 2L, "n" to 20L),
        ))
    }

    @Test
    fun `test FlightSQL bulk ingest within an FSQL transaction`() {
        val txn = fsqlClient.beginTransaction(*emptyCallOpts)
        usersRoot(listOf(1L to 100L)) { root ->
            fsqlClient.executeIngest(root, ingestOpts("users"), txn, *emptyCallOpts)
        }
        fsqlClient.commit(txn, *emptyCallOpts)

        assertUsersTableContains("users", listOf(mapOf("_id" to 1L, "n" to 100L)))
    }

    @Test
    fun `test FlightSQL bulk ingest rolled back doesn't persist`() {
        usersRoot(listOf(1L to 1L)) { root ->
            fsqlClient.executeIngest(root, ingestOpts("users"), *emptyCallOpts)
        }

        val txn = fsqlClient.beginTransaction(*emptyCallOpts)
        usersRoot(listOf(99L to 999L)) { root ->
            fsqlClient.executeIngest(root, ingestOpts("users"), txn, *emptyCallOpts)
        }
        fsqlClient.rollback(txn, *emptyCallOpts)

        assertUsersTableContains("users", listOf(mapOf("_id" to 1L, "n" to 1L)))
    }

    @Test
    fun `test FlightSQL bulk ingest honours cmd_schema`() {
        usersRoot(listOf(1L to 10L)) { root ->
            fsqlClient.executeIngest(root, ingestOpts("users", schema = "my_schema"), *emptyCallOpts)
        }
        assertUsersTableContains("my_schema.users", listOf(mapOf("_id" to 1L, "n" to 10L)))
    }

    private fun assertIngestRejected(
        opts: ExecuteIngestOptions,
        descriptionContains: String,
    ) {
        usersRoot(listOf(1L to 0L)) { root ->
            val ex = assertThrows(FlightRuntimeException::class.java) {
                fsqlClient.executeIngest(root, opts, *emptyCallOpts)
            }
            assertTrue(
                ex.message?.contains(descriptionContains) == true,
                "expected message containing '$descriptionContains', got: ${ex.message}"
            )
        }
    }

    @Test
    fun `test FlightSQL bulk ingest rejects per-call catalog override`() {
        assertIngestRejected(ingestOpts("users", catalog = "some_other_catalog"), "catalog")
    }

    @Test
    fun `test FlightSQL bulk ingest rejects fail-if-not-exist`() {
        val tdo = TableDefinitionOptions.newBuilder()
            .setIfNotExist(TableNotExistOption.TABLE_NOT_EXIST_OPTION_FAIL)
            .setIfExists(TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
            .build()
        assertIngestRejected(ingestOpts("users", tdo = tdo), "fail-if-not-exist")
    }

    @Test
    fun `test FlightSQL bulk ingest rejects fail-if-exists`() {
        val tdo = TableDefinitionOptions.newBuilder()
            .setIfNotExist(TableNotExistOption.TABLE_NOT_EXIST_OPTION_CREATE)
            .setIfExists(TableExistsOption.TABLE_EXISTS_OPTION_FAIL)
            .build()
        assertIngestRejected(ingestOpts("users", tdo = tdo), "append-on-exists")
    }

    @Test
    fun `test FlightSQL bulk ingest rejects dotted table name`() {
        assertIngestRejected(ingestOpts("my_schema.users"), "must not contain '.'")
    }

    private fun multiBatchIngest(table: String, batches: List<List<Pair<Long, Long>>>) {
        val cmd = CommandStatementIngest.newBuilder()
            .setTable(table)
            .setTableDefinitionOptions(defaultIngestTdo())
            .build()
        val descriptor = FlightDescriptor.command(ProtoAny.pack(cmd).toByteArray())
        VectorSchemaRoot.create(usersSchema, al).use { root ->
            val idVec = root.getVector("_id") as BigIntVector
            val nVec = root.getVector("n") as BigIntVector
            val listener = flightClient.startPut(descriptor, root, AsyncPutListener())
            for (batch in batches) {
                batch.forEachIndexed { i, (id, n) -> idVec.setSafe(i, id); nVec.setSafe(i, n) }
                root.rowCount = batch.size
                listener.putNext()
            }
            listener.completed()
            listener.getResult()
        }
    }

    @Test
    fun `test FlightSQL bulk ingest streams multiple batches`() {
        multiBatchIngest("users", listOf(
            listOf(1L to 10L, 2L to 20L),
            listOf(3L to 30L, 4L to 40L),
        ))
        assertUsersTableContains("users", listOf(
            mapOf("_id" to 1L, "n" to 10L),
            mapOf("_id" to 2L, "n" to 20L),
            mapOf("_id" to 3L, "n" to 30L),
            mapOf("_id" to 4L, "n" to 40L),
        ))
    }

    // -- Error handling --

    @Test
    fun `test ADBC getObjects at ALL depth doesn't crash on IPC parse`() {
        insertData("INSERT INTO users (_id, name) VALUES (1, 'jms')")

        conn.getObjects(GetObjectsDepth.ALL, null, "public", "users", null, null).use { rdr ->
            assertTrue(rdr.loadNextBatch(), "Expected at least one batch")
            assertTrue(rdr.vectorSchemaRoot.rowCount > 0, "Expected catalog rows")
        }
    }

    private fun Any?.asList() = this as List<*>
    private fun Any?.asMap() = this as Map<*, *>

    @Test
    fun `test ADBC getObjects at ALL depth populates table_columns`() {
        insertData("INSERT INTO users (_id, name) VALUES (1, 'jms')")

        conn.getObjects(GetObjectsDepth.ALL, null, "public", "users", null, null).use { rdr ->
            assertTrue(rdr.loadNextBatch())
            val root = rdr.vectorSchemaRoot

            val catalogDbSchemas = (root.getVector("catalog_db_schemas") as ListVector).getObject(0).asList()
            val publicSchema = catalogDbSchemas.first { it.asMap()["db_schema_name"].toString() == "public" }.asMap()
            val tables = publicSchema["db_schema_tables"].asList()
            val usersTable = tables.first { it.asMap()["table_name"].toString() == "users" }.asMap()
            val columns = usersTable["table_columns"].asList()
            val columnNames = columns.map { it.asMap()["column_name"].toString() }.toSet()
            assertEquals(setOf("_id", "name"), columnNames)
        }
    }

    @Test
    fun `test FlightSQL getTables with includeSchema returns columns`() {
        insertData("INSERT INTO users (_id, name) VALUES (1, 'jms')")

        val info = fsqlClient.getTables(null, null, "users", null, true, *emptyCallOpts)
        val ticket = info.endpoints.first().ticket
        fsqlClient.getStream(ticket, *emptyCallOpts).use { stream ->
            assertTrue(stream.next())
            val root = stream.root
            assertTrue(root.rowCount > 0, "Expected at least one table row")

            val schemaVec = root.getVector("table_schema") as VarBinaryVector
            val rowIdx = (0 until root.rowCount).first {
                (root.getVector("table_name").getObject(it) as? Text)?.toString() == "users"
            }
            val schemaBytes = schemaVec.getObject(rowIdx) ?: error("table_schema bytes were null")

            val tableSchema = schemaBytes.deserialiseArrowSchema()
            assertEquals(setOf("_id", "name"), tableSchema.fields.map { it.name }.toSet())
        }
    }

    private fun ByteArray.deserialiseArrowSchema(): Schema =
        MessageSerializer.deserializeSchema(ReadChannel(Channels.newChannel(ByteArrayInputStream(this))))

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
