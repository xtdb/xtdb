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
import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.CallInfo
import org.apache.arrow.flight.CallStatus
import org.apache.arrow.flight.CloseSessionRequest
import org.apache.arrow.flight.CloseSessionResult
import org.apache.arrow.flight.FlightCallHeaders
import org.apache.arrow.flight.FlightClientMiddleware
import org.apache.arrow.flight.GetSessionOptionsRequest
import org.apache.arrow.flight.HeaderCallOption
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.NoOpSessionOptionValueVisitor
import org.apache.arrow.flight.SessionOptionValueFactory
import org.apache.arrow.flight.SetSessionOptionsRequest
import org.apache.arrow.flight.SetSessionOptionsResult
import org.apache.arrow.flight.client.ClientCookieMiddleware
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.flight.sql.FlightSqlClient.ExecuteIngestOptions
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedTransaction
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
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
import xtdb.database.Database
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
    private var flightPort: Int = -1

    private val emptyCallOpts = arrayOf<CallOption>()

    @BeforeEach
    fun setUp() {
        xtdb = Xtdb.openNode {
            server { port = 0 }
            flightSql { port = 0 }
        }

        flightPort = xtdb.flightSqlPort

        al = RootAllocator()
        db = FlightSqlDriver(al).open(mapOf("uri" to "grpc+tcp://127.0.0.1:${flightPort}"))
        conn = db.connect()

        flightClient = FlightClient.builder(al, Location.forGrpcInsecure("127.0.0.1", flightPort)).build()
        fsqlClient = FlightSqlClient(flightClient)
    }

    /**
     * A FlightSQL client that carries the `arrow_flight_session_id` cookie across calls
     * (the ClientCookieMiddleware) — mirrors how the real ADBC drivers behave, so the
     * server-side ServerSessionMiddleware sees a stable session across the call sequence.
     */
    private fun cookieAwareClient(): FlightSqlClient =
        FlightSqlClient(
            FlightClient.builder(al, Location.forGrpcInsecure("127.0.0.1", flightPort))
                .intercept(ClientCookieMiddleware.Factory())
                .build()
        )

    /** A second cookieless FlightSQL client (distinct from `fsqlClient`). */
    private fun plainClient(): FlightSqlClient =
        FlightSqlClient(
            FlightClient.builder(al, Location.forGrpcInsecure("127.0.0.1", flightPort)).build()
        )

    @AfterEach
    fun tearDown() {
        fsqlClient.close()
        conn.close()
        db.close()
        al.close()
        xtdb.close()
    }

    private fun FlightInfo.readRows(client: FlightSqlClient = fsqlClient): List<Map<*, *>> {
        val ticket = endpoints.first().ticket
        client.getStream(ticket, *emptyCallOpts).use { stream ->
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
    fun `test ADBC parameterSchema reports positional placeholders as empty-named NullType fields`() {
        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT ?, ?, ?")
            stmt.prepare()
            // per ADBC: unnamed positional params → empty name; undeterminable type → NA (NullType).
            // asserted on the wire path because both round-trip through IPC schema serialisation.
            assertEquals(listOf("", "", ""), stmt.parameterSchema.fields.map { it.name })
            assertEquals(List(3) { ArrowType.Null.INSTANCE }, stmt.parameterSchema.fields.map { it.type })
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

    private fun attachNewDbWithWidgets() {
        xtdb.connect().use { it.attachDb("new_db", Database.Config()) }
        xtdb.connect("new_db").use { c ->
            c.createStatement().use { it.setSqlQuery("INSERT INTO widgets (_id, w) VALUES (1, 2)"); it.executeUpdate() }
        }
    }

    @Test
    fun `getSchemas and getTables span catalogs`() {
        insertData("INSERT INTO tables_test (_id) VALUES (1)")
        attachNewDbWithWidgets()

        val schemaRows = fsqlClient.getSchemas(null, null, *emptyCallOpts).readRows()
        assertTrue(
            schemaRows.any { it["catalog_name"] == "new_db" && it["db_schema_name"] == "public" },
            "new_db's schemas appear over the wire: $schemaRows"
        )

        val widgetRows = fsqlClient.getTables(null, null, null, null, false, *emptyCallOpts)
            .readRows().filter { it["table_name"] == "widgets" }
        assertEquals(
            listOf("new_db"), widgetRows.map { it["catalog_name"] },
            "widgets is reported under new_db, not the connection's own catalog"
        )

        val newDbOnly = fsqlClient.getTables("new_db", null, null, null, false, *emptyCallOpts).readRows()
        assertEquals(
            setOf("new_db"), newDbOnly.map { it["catalog_name"] }.toSet(),
            "an exact catalog filter restricts to that catalog"
        )
        assertTrue(newDbOnly.none { it["table_name"] == "tables_test" })
    }

    @Test
    fun `getTables includeSchema resolves a table in another catalog`() {
        attachNewDbWithWidgets()

        val info = fsqlClient.getTables("new_db", null, "widgets", null, true, *emptyCallOpts)
        fsqlClient.getStream(info.endpoints.first().ticket, *emptyCallOpts).use { stream ->
            assertTrue(stream.next())
            val root = stream.root
            val schemaVec = root.getVector("table_schema") as VarBinaryVector
            val rowIdx = (0 until root.rowCount).first {
                (root.getVector("table_name").getObject(it) as? Text)?.toString() == "widgets"
            }
            val bytes = schemaVec.getObject(rowIdx) ?: error("table_schema bytes were null")
            assertEquals(setOf("_id", "w"), bytes.deserialiseArrowSchema().fields.map { it.name }.toSet())
        }
    }

    @Test
    fun `getSchemas db_schema_filter_pattern excludes non-matching schemas`() {
        insertData("INSERT INTO foo (_id) VALUES (1)")
        insertData("INSERT INTO reporting.bar (_id) VALUES (1)")

        val schemaNames = fsqlClient.getSchemas(null, "reporting", *emptyCallOpts).readRows()
            .map { it["db_schema_name"] }.toSet()
        assertEquals(setOf("reporting"), schemaNames)
    }

    @Test
    fun `getTables filters by db_schema and table_name patterns`() {
        insertData("INSERT INTO foo (_id) VALUES (1)")
        insertData("INSERT INTO reporting.bar (_id) VALUES (1)")

        val bySchema = fsqlClient.getTables(null, "reporting", null, null, false, *emptyCallOpts)
            .readRows().map { it["table_name"] }.toSet()
        assertEquals(setOf("bar"), bySchema)

        val byName = fsqlClient.getTables(null, null, "foo", null, false, *emptyCallOpts)
            .readRows().map { it["table_name"] }.toSet()
        assertEquals(setOf("foo"), byName)
    }

    @Test
    fun `getTables with an empty catalog string matches no catalog`() {
        insertData("INSERT INTO foo (_id) VALUES (1)")

        // FlightSQL: an empty catalog string selects objects "without a catalog"; XTDB has none.
        val rows = fsqlClient.getTables("", null, null, null, false, *emptyCallOpts).readRows()
        assertTrue(rows.isEmpty(), "empty catalog matches nothing: $rows")
    }

    @Test
    fun `test FlightSQL getSqlInfo`() {
        val rows = fsqlClient.getSqlInfo(intArrayOf(), *emptyCallOpts).readRows()
        assertTrue(rows.isNotEmpty(), "Expected at least one info row")
    }

    @Test
    fun `test FlightSQL getSqlInfo advertises transaction support`() {
        // The ADBC Go driver (underlying the Python adbc_driver_flightsql package) requests
        // FLIGHT_SQL_SERVER_TRANSACTION at connect; if it's absent set_autocommit(False) raises
        // NOT_IMPLEMENTED.
        // Value 1 = SqlSupportedTransaction.TRANSACTION (begin/commit/rollback supported).
        //
        // We read the `value` DenseUnionVector with *standard* Arrow accessors off the post-IPC stream
        // root — the way the Go/Python consumer reads it — rather than through XTDB's lenient Relation
        // reader, and we request it in a mixed batch alongside the string-valued codes so a botched
        // dense-union offset for the int32 leg would surface as a null here.
        val code = SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION_VALUE
        val info = fsqlClient.getSqlInfo(
            intArrayOf(SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE, SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE, code),
            *emptyCallOpts
        )
        fsqlClient.getStream(info.endpoints.first().ticket, *emptyCallOpts).use { stream ->
            assertTrue(stream.next(), "expected a GET_SQL_INFO batch")
            val root = stream.root

            val infoNameVec = root.getVector("info_name") as UInt4Vector
            val valueVec = root.getVector("value") as DenseUnionVector

            val rowIdx = (0 until root.rowCount).singleOrNull { infoNameVec.get(it) == code }
                ?: fail("GET_SQL_INFO must advertise FLIGHT_SQL_SERVER_TRANSACTION (code $code)")

            // type id 3 = int32_bitmask leg of the GET_SQL_INFO value union
            assertEquals(3, valueVec.getTypeId(rowIdx), "code $code must be tagged with the int32 leg")

            val int32Vec = valueVec.getIntVector(3)
            val legOffset = valueVec.getOffset(rowIdx)
            assertFalse(int32Vec.isNull(legOffset), "code $code's int32 value must be non-null on the wire")
            assertEquals(
                SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_TRANSACTION_VALUE,
                int32Vec.get(legOffset),
                "code $code must carry SqlSupportedTransaction.TRANSACTION"
            )
        }
    }

    private val asString = object : NoOpSessionOptionValueVisitor<String?>() {
        override fun visit(value: String) = value
    }

    @Test
    fun `test FlightSQL getSessionOptions returns catalog and schema`() {
        val result = fsqlClient.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)

        val opts = result.sessionOptions
        assertEquals("xtdb", opts["catalog"]?.acceptVisitor(asString))
        assertEquals("public", opts["schema"]?.acceptVisitor(asString))
    }

    @Test
    fun `test FlightSQL setSessionOptions rejects unknown option and bad catalog`() {
        cookieAwareClient().use { client ->
            val result = client.setSessionOptions(
                SetSessionOptionsRequest(
                    mapOf(
                        "bogus" to SessionOptionValueFactory.makeSessionOptionValue("x"),
                        "catalog" to SessionOptionValueFactory.makeSessionOptionValue("no-such-db"),
                    )
                ),
                *emptyCallOpts
            )
            assertTrue(result.hasErrors())
            assertEquals(
                SetSessionOptionsResult.ErrorValue.INVALID_NAME,
                result.errors["bogus"]?.value
            )
            assertEquals(
                SetSessionOptionsResult.ErrorValue.INVALID_VALUE,
                result.errors["catalog"]?.value
            )
        }
    }

    @Test
    fun `test FlightSQL set then get catalog round-trips within a session`() {
        val dbCat = (xtdb as XtdbInternal).dbCatalog
        dbCat.attach("other-db", null)

        cookieAwareClient().use { client ->
            assertEquals(
                "xtdb",
                client.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)
                    .sessionOptions["catalog"]?.acceptVisitor(asString)
            )

            val setResult = client.setSessionOptions(
                SetSessionOptionsRequest(
                    mapOf("catalog" to SessionOptionValueFactory.makeSessionOptionValue("other-db"))
                ),
                *emptyCallOpts
            )
            assertFalse(setResult.hasErrors(), "set catalog should succeed: ${setResult.errors}")

            assertEquals(
                "other-db",
                client.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)
                    .sessionOptions["catalog"]?.acceptVisitor(asString)
            )
        }
    }

    @Test
    fun `test FlightSQL catalog session option routes queries to that database`() {
        val dbCat = (xtdb as XtdbInternal).dbCatalog
        dbCat.attach("other-db", null)

        cookieAwareClient().use { client ->
            client.setSessionOptions(
                SetSessionOptionsRequest(
                    mapOf("catalog" to SessionOptionValueFactory.makeSessionOptionValue("other-db"))
                ),
                *emptyCallOpts
            )

            assertEquals(
                -1L,
                client.executeUpdate("INSERT INTO t (_id, n) VALUES (1, 'a')", *emptyCallOpts)
            )
            assertEquals(
                listOf(mapOf("_id" to 1L, "n" to "a")),
                client.execute("SELECT _id, n FROM t", *emptyCallOpts).readRows()
            )

            fsqlClient.executeUpdate("CREATE TABLE t (_id, n)", *emptyCallOpts)
            assertEquals(
                emptyList<Map<*, *>>(),
                fsqlClient.execute("SELECT _id, n FROM t", *emptyCallOpts).readRows()
            )
        }
    }

    @Test
    fun `test FlightSQL closeSession invalidates the session`() {
        cookieAwareClient().use { client ->
            // setSessionOptions is the mutating RPC that mints the session
            client.setSessionOptions(
                SetSessionOptionsRequest(
                    mapOf("catalog" to SessionOptionValueFactory.makeSessionOptionValue("xtdb"))
                ),
                *emptyCallOpts
            )

            val result = client.closeSession(CloseSessionRequest(), *emptyCallOpts)
            assertEquals(CloseSessionResult.Status.CLOSED, result.status)
        }
    }

    @Test
    fun `test FlightSQL closeSession with no session errors NOT_FOUND`() {
        val ex = assertThrows(FlightRuntimeException::class.java) {
            fsqlClient.closeSession(CloseSessionRequest(), *emptyCallOpts)
        }
        assertEquals(org.apache.arrow.flight.FlightStatusCode.NOT_FOUND, ex.status().code())
    }

    private fun dbCallOpts(db: String): Array<CallOption> =
        arrayOf(HeaderCallOption(FlightCallHeaders().apply { insert("x-xtdb-database", db) }))

    private fun catalogOpt(db: String) =
        SetSessionOptionsRequest(mapOf("catalog" to SessionOptionValueFactory.makeSessionOptionValue(db)))

    @Test
    fun `test FlightSQL closeSession aborts an open session-bound transaction`() {
        fsqlClient.executeUpdate("CREATE TABLE users (_id, n)", *emptyCallOpts)

        cookieAwareClient().use { client ->
            // session must exist before beginTransaction for the tx to be session-bound
            client.setSessionOptions(catalogOpt("xtdb"), *emptyCallOpts)

            val txn = client.beginTransaction(*emptyCallOpts)
            client.executeUpdate("INSERT INTO users (_id, n) VALUES (1, 'in-tx')", txn, *emptyCallOpts)

            client.closeSession(CloseSessionRequest(), *emptyCallOpts)

            // closeSession reclaimed the tx connection, so the handle is now unknown
            val ex = assertThrows(FlightRuntimeException::class.java) {
                client.commit(txn, *emptyCallOpts)
            }
            assertEquals(org.apache.arrow.flight.FlightStatusCode.NOT_FOUND, ex.status().code())
        }

        assertEquals(
            emptyList<Map<*, *>>(),
            fsqlClient.execute("SELECT _id, n FROM users", *emptyCallOpts).readRows()
        )
    }

    @Test
    fun `test FlightSQL endTransaction on a stale handle errors NOT_FOUND`() {
        val txn = fsqlClient.beginTransaction(*emptyCallOpts)
        fsqlClient.commit(txn, *emptyCallOpts)

        val ex = assertThrows(FlightRuntimeException::class.java) {
            fsqlClient.commit(txn, *emptyCallOpts)
        }
        assertEquals(org.apache.arrow.flight.FlightStatusCode.NOT_FOUND, ex.status().code())
    }

    @Test
    fun `test FlightSQL a transaction is scoped to its own session`() {
        cookieAwareClient().use { sessionA ->
            cookieAwareClient().use { sessionB ->
                // each client establishes its own session
                sessionA.setSessionOptions(catalogOpt("xtdb"), *emptyCallOpts)
                sessionB.setSessionOptions(catalogOpt("xtdb"), *emptyCallOpts)

                val txnA = sessionA.beginTransaction(*emptyCallOpts)

                // session B may not drive session A's transaction - it doesn't exist for B
                val onCommit = assertThrows(FlightRuntimeException::class.java) {
                    sessionB.commit(txnA, *emptyCallOpts)
                }
                assertEquals(org.apache.arrow.flight.FlightStatusCode.NOT_FOUND, onCommit.status().code())

                val onDml = assertThrows(FlightRuntimeException::class.java) {
                    sessionB.executeUpdate("INSERT INTO users (_id, n) VALUES (1, 'x')", txnA, *emptyCallOpts)
                }
                assertEquals(org.apache.arrow.flight.FlightStatusCode.NOT_FOUND, onDml.status().code())

                // the owning session can still use and commit it
                sessionA.executeUpdate("INSERT INTO users (_id, n) VALUES (2, 'a')", txnA, *emptyCallOpts)
                sessionA.commit(txnA, *emptyCallOpts)
            }
        }

        assertEquals(
            listOf(mapOf("_id" to 2L, "n" to "a")),
            fsqlClient.execute("SELECT _id, n FROM users", *emptyCallOpts).readRows()
        )
    }

    @Test
    fun `test FlightSQL empty catalog erases the session option back to default`() {
        val dbCat = (xtdb as XtdbInternal).dbCatalog
        dbCat.attach("other-db", null)

        cookieAwareClient().use { client ->
            client.setSessionOptions(catalogOpt("other-db"), *emptyCallOpts)
            assertEquals(
                "other-db",
                client.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)
                    .sessionOptions["catalog"]?.acceptVisitor(asString)
            )

            client.setSessionOptions(catalogOpt(""), *emptyCallOpts)
            assertEquals(
                "xtdb",
                client.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)
                    .sessionOptions["catalog"]?.acceptVisitor(asString)
            )
        }
    }

    @Test
    fun `test FlightSQL setSessionOptions accepts schema public and rejects other schemas`() {
        cookieAwareClient().use { client ->
            val ok = client.setSessionOptions(
                SetSessionOptionsRequest(
                    mapOf("schema" to SessionOptionValueFactory.makeSessionOptionValue("public"))
                ),
                *emptyCallOpts
            )
            assertFalse(ok.hasErrors(), "schema=public should be a confirming no-op: ${ok.errors}")

            val bad = client.setSessionOptions(
                SetSessionOptionsRequest(
                    mapOf("schema" to SessionOptionValueFactory.makeSessionOptionValue("other"))
                ),
                *emptyCallOpts
            )
            assertEquals(
                SetSessionOptionsResult.ErrorValue.INVALID_VALUE,
                bad.errors["schema"]?.value
            )
        }
    }

    @Test
    fun `test FlightSQL two sessions keep independent catalog and data`() {
        val dbCat = (xtdb as XtdbInternal).dbCatalog
        dbCat.attach("other-db", null)

        cookieAwareClient().use { sessionA ->
            cookieAwareClient().use { sessionB ->
                sessionA.setSessionOptions(catalogOpt("other-db"), *emptyCallOpts)
                // sessionB deliberately left on the default catalog

                assertEquals(
                    "other-db",
                    sessionA.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)
                        .sessionOptions["catalog"]?.acceptVisitor(asString)
                )
                assertEquals(
                    "xtdb",
                    sessionB.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)
                        .sessionOptions["catalog"]?.acceptVisitor(asString)
                )

                sessionA.executeUpdate("INSERT INTO t (_id, n) VALUES (1, 'A')", *emptyCallOpts)
                sessionB.executeUpdate("INSERT INTO t (_id, n) VALUES (2, 'B')", *emptyCallOpts)

                assertEquals(
                    listOf(mapOf("_id" to 1L, "n" to "A")),
                    sessionA.execute("SELECT _id, n FROM t", *emptyCallOpts).readRows()
                )
                assertEquals(
                    listOf(mapOf("_id" to 2L, "n" to "B")),
                    sessionB.execute("SELECT _id, n FROM t", *emptyCallOpts).readRows()
                )
            }
        }
    }

    @Test
    fun `test FlightSQL x-xtdb-database header beats the session catalog option`() {
        val dbCat = (xtdb as XtdbInternal).dbCatalog
        dbCat.attach("other-db", null)

        cookieAwareClient().use { client ->
            client.setSessionOptions(catalogOpt("other-db"), *emptyCallOpts)

            // header must override the session catalog: write targets xtdb, not other-db
            client.executeUpdate("INSERT INTO t (_id, n) VALUES (1, 'hdr')", *dbCallOpts("xtdb"))

            assertEquals(
                listOf(mapOf("_id" to 1L, "n" to "hdr")),
                client.execute("SELECT _id, n FROM t", *dbCallOpts("xtdb")).readRows()
            )
            client.executeUpdate("CREATE TABLE t (_id, n)", *emptyCallOpts)
            assertEquals(
                emptyList<Map<*, *>>(),
                client.execute("SELECT _id, n FROM t", *emptyCallOpts).readRows()
            )
        }
    }

    private class SetCookieRecorder : FlightClientMiddleware.Factory {
        val values = mutableListOf<String>()

        override fun onCallStarted(info: CallInfo) = object : FlightClientMiddleware {
            override fun onBeforeSendingHeaders(outgoingHeaders: CallHeaders) {}
            override fun onHeadersReceived(incomingHeaders: CallHeaders) {
                incomingHeaders.getAll("set-cookie").forEach(values::add)
            }
            override fun onCallCompleted(status: CallStatus) {}
        }
    }

    @Test
    fun `test FlightSQL getSessionOptions does not mint a session`() {
        val recorder = SetCookieRecorder()
        FlightSqlClient(
            FlightClient.builder(al, Location.forGrpcInsecure("127.0.0.1", flightPort))
                .intercept(recorder)
                .build()
        ).use { client ->
            client.getSessionOptions(GetSessionOptionsRequest(), *emptyCallOpts)
            assertEquals(emptyList<String>(), recorder.values)

            client.setSessionOptions(
                SetSessionOptionsRequest(
                    mapOf("catalog" to SessionOptionValueFactory.makeSessionOptionValue("xtdb"))
                ),
                *emptyCallOpts
            )
            assertTrue(
                recorder.values.any { it.startsWith("arrow_flight_session_id=") },
                "setSessionOptions should establish the Flight SQL session cookie; saw ${recorder.values}"
            )
        }
    }

    @Test
    fun `test FlightSQL transaction does not capture another cookieless clients autocommit write`() {
        val txn = fsqlClient.beginTransaction(*emptyCallOpts)
        try {
            assertEquals(
                -1L,
                fsqlClient.executeUpdate("INSERT INTO users (_id, n) VALUES (1, 'tx')", txn, *emptyCallOpts)
            )

            plainClient().use { other ->
                assertEquals(
                    -1L,
                    other.executeUpdate("INSERT INTO users (_id, n) VALUES (2, 'auto')", *emptyCallOpts)
                )
            }

            assertEquals(
                listOf(mapOf("_id" to 2L, "n" to "auto")),
                fsqlClient.execute("SELECT _id, n FROM users ORDER BY _id", *emptyCallOpts).readRows()
            )

            fsqlClient.commit(txn, *emptyCallOpts)

            assertEquals(
                listOf(
                    mapOf("_id" to 1L, "n" to "tx"),
                    mapOf("_id" to 2L, "n" to "auto"),
                ),
                fsqlClient.execute("SELECT _id, n FROM users ORDER BY _id", *emptyCallOpts).readRows()
            )
        } catch (t: Throwable) {
            runCatching { fsqlClient.rollback(txn, *emptyCallOpts) }
            throw t
        }
    }

    @Test
    fun `test FlightSQL open transaction does not poison autocommit path`() {
        val txn = fsqlClient.beginTransaction(*emptyCallOpts)
        try {
            assertEquals(
                -1L,
                fsqlClient.executeUpdate("INSERT INTO users (_id, n) VALUES (1, 'tx')", txn, *emptyCallOpts)
            )

            assertEquals(
                -1L,
                fsqlClient.executeUpdate("INSERT INTO users (_id, n) VALUES (2, 'auto')", *emptyCallOpts)
            )

            assertEquals(
                listOf(mapOf("_id" to 2L, "n" to "auto")),
                fsqlClient.execute("SELECT _id, n FROM users ORDER BY _id", *emptyCallOpts).readRows()
            )
        } finally {
            runCatching { fsqlClient.rollback(txn, *emptyCallOpts) }
        }
    }

    @Test
    fun `test FlightSQL double begin uses isolated transaction connections`() {
        val tx1 = fsqlClient.beginTransaction(*emptyCallOpts)
        val tx2 = fsqlClient.beginTransaction(*emptyCallOpts)
        try {
            assertEquals(-1L, fsqlClient.executeUpdate("INSERT INTO users (_id, n) VALUES (1, 'commit')", tx1, *emptyCallOpts))
            assertEquals(-1L, fsqlClient.executeUpdate("INSERT INTO users (_id, n) VALUES (2, 'rollback')", tx2, *emptyCallOpts))

            fsqlClient.commit(tx1, *emptyCallOpts)
            fsqlClient.rollback(tx2, *emptyCallOpts)

            assertEquals(
                listOf(mapOf("_id" to 1L, "n" to "commit")),
                fsqlClient.execute("SELECT _id, n FROM users ORDER BY _id", *emptyCallOpts).readRows()
            )
        } catch (t: Throwable) {
            runCatching { fsqlClient.rollback(tx1, *emptyCallOpts) }
            runCatching { fsqlClient.rollback(tx2, *emptyCallOpts) }
            throw t
        }
    }

    // -- executeSchema --

    @Test
    fun `test ADBC executeSchema on prepared query`() {
        insertData("INSERT INTO foo RECORDS {_id: 1, n: 'one'}")

        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT _id, n FROM foo")
            stmt.prepare()
            assertEquals(listOf("_id", "n"), stmt.executeSchema().fields.map { it.name })
        }
    }

    @Test
    fun `test ADBC executeSchema on ad-hoc query`() {
        insertData("INSERT INTO foo RECORDS {_id: 1, n: 'one'}")

        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT _id, n FROM foo")
            assertEquals(listOf("_id", "n"), stmt.executeSchema().fields.map { it.name })
        }
    }

    @Test
    fun `test FlightSQL getExecuteSchema`() {
        insertData("INSERT INTO foo RECORDS {_id: 1, n: 'one'}")

        val result = fsqlClient.getExecuteSchema("SELECT _id, n FROM foo", *emptyCallOpts)
        assertEquals(listOf("_id", "n"), result.schema.fields.map { it.name })
    }

    @Test
    fun `test FlightSQL getExecuteSchema rejects DML`() {
        val ex = assertThrows(FlightRuntimeException::class.java) {
            fsqlClient.getExecuteSchema("INSERT INTO foo RECORDS {_id: 1}", *emptyCallOpts)
        }
        assertTrue(
            ex.message?.contains("only supports queries") == true,
            "expected message containing 'only supports queries', got: ${ex.message}"
        )
    }

    @Test
    fun `test FlightSQL fetchSchema on prepared query`() {
        insertData("INSERT INTO foo RECORDS {_id: 1, n: 'one'}")

        fsqlClient.prepare("SELECT _id, n FROM foo", *emptyCallOpts).use { prepared ->
            assertEquals(listOf("_id", "n"), prepared.fetchSchema(*emptyCallOpts).schema.fields.map { it.name })
        }
    }

    @Test
    fun `test same-connection INSERT then executeSchema sees real column names`() {
        conn.createStatement().use { ins ->
            ins.setSqlQuery("INSERT INTO same_conn_repro (_id, n) VALUES (1, 100)")
            ins.executeUpdate()
        }
        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT n FROM same_conn_repro")
            stmt.prepare()
            assertEquals(listOf("n"), stmt.executeSchema().fields.map { it.name })
        }
    }

    @Test
    fun `test FlightSQL session reads its own writes and stays isolated from another session`() {
        cookieAwareClient().use { sessionA ->
            cookieAwareClient().use { sessionB ->
                sessionA.setSessionOptions(catalogOpt("xtdb"), *emptyCallOpts)
                sessionB.setSessionOptions(catalogOpt("xtdb"), *emptyCallOpts)

                sessionA.executeUpdate("INSERT INTO ryw (_id, n) VALUES (1, 'a')", *emptyCallOpts)
                assertEquals(
                    listOf(mapOf("_id" to 1L, "n" to "a")),
                    sessionA.execute("SELECT _id, n FROM ryw", *emptyCallOpts).readRows(sessionA),
                    "session A must read its own write back"
                )

                sessionB.executeUpdate("INSERT INTO ryw (_id, n) VALUES (2, 'b')", *emptyCallOpts)
                assertEquals(
                    listOf(mapOf("_id" to 2L, "n" to "b")),
                    sessionB.execute("SELECT _id, n FROM ryw WHERE _id = 2", *emptyCallOpts).readRows(sessionB),
                    "session B must read its own write back without coupling to session A"
                )
            }
        }

        assertEquals(
            listOf(mapOf("_id" to 1L, "n" to "a"), mapOf("_id" to 2L, "n" to "b")),
            fsqlClient.execute("SELECT _id, n FROM ryw ORDER BY _id", *emptyCallOpts).readRows()
        )
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
