package xtdb

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation

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

    @Test
    fun `prepare and execute simple query`() {
        insertData("INSERT INTO foo RECORDS {_id: 1, n: 'one'}")

        xtdb.connect().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id, n FROM foo")
                stmt.prepare()

                stmt.executeQuery().reader.use { rdr ->
                    assertTrue(rdr.loadNextBatch())
                    Relation.fromRoot(al, rdr.vectorSchemaRoot).use { rel ->
                        assertEquals(
                            listOf(mapOf("_id" to 1L, "n" to "one")),
                            rel.toMaps(SNAKE_CASE_STRING)
                        )
                    }
                    assertFalse(rdr.loadNextBatch())
                }
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

                val schema = Schema(listOf(
                    Field("\$0", FieldType.notNullable(Types.MinorType.BIGINT.type), null)
                ))
                VectorSchemaRoot.create(schema, al).use { params ->
                    params.allocateNew()
                    (params.getVector("\$0") as BigIntVector).apply {
                        setSafe(0, 2L)
                        valueCount = 1
                    }
                    params.rowCount = 1

                    stmt.bind(params)

                    stmt.executeQuery().reader.use { rdr ->
                        assertTrue(rdr.loadNextBatch())
                        Relation.fromRoot(al, rdr.vectorSchemaRoot).use { rel ->
                            assertEquals(
                                listOf(mapOf("_id" to 2L)),
                                rel.toMaps(SNAKE_CASE_STRING)
                            )
                        }
                        assertFalse(rdr.loadNextBatch())
                    }
                }
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

                val schema = Schema(listOf(
                    Field("\$0", FieldType.notNullable(Types.MinorType.BIGINT.type), null)
                ))
                VectorSchemaRoot.create(schema, al).use { params ->
                    params.allocateNew()
                    (params.getVector("\$0") as BigIntVector).apply {
                        setSafe(0, 42L)
                        valueCount = 1
                    }
                    params.rowCount = 1

                    stmt.bind(params)
                    stmt.executeUpdate()
                }
            }

            conn.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT _id FROM foo")
                stmt.executeQuery().reader.use { rdr ->
                    assertTrue(rdr.loadNextBatch())
                    Relation.fromRoot(al, rdr.vectorSchemaRoot).use { rel ->
                        assertEquals(
                            listOf(mapOf("_id" to 42L)),
                            rel.toMaps(SNAKE_CASE_STRING)
                        )
                    }
                }
            }
        }
    }
}
