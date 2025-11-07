package xtdb.adbc

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import xtdb.api.Xtdb

class AdbcSimpleTest {

    private lateinit var xtdb: Xtdb

    private lateinit var al: BufferAllocator
    private lateinit var db: AdbcDatabase
    private lateinit var conn: AdbcConnection

    @BeforeEach
    fun setUp() {
        xtdb = Xtdb.openNode()

        al = RootAllocator()
        db = XtdbDriver(al).open(mutableMapOf(
            XtdbDriver.PARAM_XTDB_NODE to xtdb
        ))
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
    fun `test simple select`() {
        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT 1 AS x, 'hello' AS y")
            stmt.executeQuery().reader.use { rdr ->
                val root = rdr.vectorSchemaRoot
                println("Schema: ${root.schema}")
                println("Loading batch...")
                val hasData = rdr.loadNextBatch()
                println("Has data: $hasData")
                if (hasData) {
                    println("Row count: ${root.rowCount}")
                }
                assertTrue(hasData)
            }
        }
    }
}
