package xtdb

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
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
}
