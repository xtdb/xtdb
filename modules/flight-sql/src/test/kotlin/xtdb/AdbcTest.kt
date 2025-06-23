package xtdb

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.FlightSqlServer
import xtdb.api.Xtdb
import xtdb.api.flightSqlServer
import xtdb.api.module
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import java.time.ZonedDateTime
import java.util.*

class AdbcTest {

    private lateinit var xtdb: Xtdb

    private lateinit var al: BufferAllocator
    private lateinit var db: AdbcDatabase
    private lateinit var conn: AdbcConnection

    @BeforeEach
    fun setUp() {
        xtdb = Xtdb.openNode {
            server { port = 0 }
            flightSqlServer { port = 0 }
        }

        val flightPort = xtdb.module<FlightSqlServer>()!!.port

        al = RootAllocator()
        db = FlightSqlDriver(al).open(mapOf("uri" to "grpc+tcp://127.0.0.1:${flightPort}"))
        conn = db.connect()
    }

    @AfterEach
    fun tearDown() {
        conn.close()
        db.close()
        al.close()

        xtdb.close()
    }

    @Test
    fun `test simple round-trip`() {
        conn.createStatement().use { stmt ->

            stmt.setSqlQuery("""
            INSERT INTO foo RECORDS 
            {
              -- https://xkcd.com/221/
              _id: UUID 'b82ae7b2-13cf-4828-858d-cd992fec9aa7',

              name: 'foo', 
              created_at: TIMESTAMP '2020-01-01T12:34:00Z'
            }""".trimIndent())

            stmt.executeUpdate()

            stmt.setSqlQuery("SELECT * FROM foo")
            stmt.executeQuery().reader.use { rdr ->
                val root = rdr.vectorSchemaRoot
                assertTrue(rdr.loadNextBatch())

                Relation.fromRoot(al,root).use { rel ->
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
}
