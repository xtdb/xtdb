package xtdb.api

import io.kotest.matchers.shouldBe
import org.apache.arrow.adbc.core.AdbcStatement.QueryResult
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class AdbcTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp(allocator: BufferAllocator) {
        this.allocator = allocator
    }

    fun QueryResult.consumeAsMaps(): List<Map<Any, *>> {
        val res = mutableListOf<Map<Any, Any?>>()
        val rdr = this.reader
        val root = rdr.vectorSchemaRoot

        while (rdr.loadNextBatch()) {
            Relation.fromRoot(allocator, root).use { res.addAll(it.toMaps(SNAKE_CASE_STRING)) }
        }

        return res
    }

    @Test
    fun `can query through AdbcDriver`() {
        AdbcDriverFactory().getDriver(allocator).open(emptyMap()).use { db ->
            db.connect().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.setSqlQuery("INSERT INTO foo RECORDS {_id: 1}, {_id: 2}, {_id: 3}")
                    stmt.executeUpdate()

                    stmt.setSqlQuery("SELECT * FROM foo ORDER BY _id")
                    stmt.executeQuery().use { res ->
                        res.consumeAsMaps() shouldBe listOf(
                            mapOf("_id" to 1L),
                            mapOf("_id" to 2L),
                            mapOf("_id" to 3L),
                        )
                    }
                }
            }
        }
    }
}