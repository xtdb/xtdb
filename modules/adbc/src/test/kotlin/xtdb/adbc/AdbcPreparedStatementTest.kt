package xtdb.adbc

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import xtdb.api.Xtdb
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation

class AdbcPreparedStatementTest {

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
    fun `test prepared statement with parameters`() {
        conn.createStatement().use { stmt ->
            // Set up a parameterized query
            stmt.setSqlQuery("SELECT ? AS x, ? AS y")

            // Prepare the statement
            stmt.prepare()

            // Create parameter values
            val paramSchema = Schema(listOf(
                Field("?_0", FieldType.nullable(ArrowType.Int(64, true)), null),
                Field("?_1", FieldType.nullable(ArrowType.Utf8()), null)
            ))

            VectorSchemaRoot.create(paramSchema, al).use { paramRoot ->
                val xVec = paramRoot.getVector("?_0") as BigIntVector
                val yVec = paramRoot.getVector("?_1") as VarCharVector

                // Set parameter values: 42, "hello"
                xVec.allocateNew(1)
                yVec.allocateNew(1)

                xVec.setSafe(0, 42L)
                yVec.setSafe(0, "hello".toByteArray())

                paramRoot.rowCount = 1

                // Bind parameters
                stmt.bind(paramRoot)

                // Execute query
                stmt.executeQuery().reader.use { rdr ->
                    val root = rdr.vectorSchemaRoot
                    assertTrue(rdr.loadNextBatch())

                    Relation.fromRoot(al, root).use { rel ->
                        val results = rel.toMaps(SNAKE_CASE_STRING)
                        assertEquals(1, results.size)

                        val row = results[0]
                        assertEquals(42L, row["x"])
                        assertEquals("hello", row["y"])
                    }

                    assertFalse(rdr.loadNextBatch())
                }
            }
        }
    }

    @Test
    fun `test prepared statement reuse with different parameters`() {
        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT ? + 10 AS result")
            stmt.prepare()

            val paramSchema = Schema(listOf(
                Field("?_0", FieldType.nullable(ArrowType.Int(64, true)), null)
            ))

            // Execute with first parameter value: 5
            VectorSchemaRoot.create(paramSchema, al).use { paramRoot ->
                val vec = paramRoot.getVector("?_0") as BigIntVector
                vec.allocateNew(1)
                vec.setSafe(0, 5L)
                paramRoot.rowCount = 1

                stmt.bind(paramRoot)
                stmt.executeQuery().reader.use { rdr ->
                    val root = rdr.vectorSchemaRoot
                    assertTrue(rdr.loadNextBatch())

                    Relation.fromRoot(al, root).use { rel ->
                        val results = rel.toMaps(SNAKE_CASE_STRING)
                        assertEquals(15L, results[0]["result"])
                    }
                }
            }

            // Execute with second parameter value: 20
            VectorSchemaRoot.create(paramSchema, al).use { paramRoot ->
                val vec = paramRoot.getVector("?_0") as BigIntVector
                vec.allocateNew(1)
                vec.setSafe(0, 20L)
                paramRoot.rowCount = 1

                stmt.bind(paramRoot)
                stmt.executeQuery().reader.use { rdr ->
                    val root = rdr.vectorSchemaRoot
                    assertTrue(rdr.loadNextBatch())

                    Relation.fromRoot(al, root).use { rel ->
                        val results = rel.toMaps(SNAKE_CASE_STRING)
                        assertEquals(30L, results[0]["result"])
                    }
                }
            }
        }
    }
}
