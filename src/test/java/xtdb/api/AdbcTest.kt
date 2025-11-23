package xtdb.api

import io.kotest.matchers.shouldBe
import org.apache.arrow.adbc.core.AdbcStatement.QueryResult
import org.apache.arrow.adbc.core.BulkIngestMode
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.adbc.XtdbConnection
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import xtdb.arrow.VectorType.Companion.UUID
import xtdb.arrow.VectorType.Companion.ofType
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

    @Test
    fun `bulkIngest with RelationReader binding`() {
        AdbcDriverFactory().getDriver(allocator).open(emptyMap()).use { db ->
            db.connect().use { conn ->
                // Create test data using Relation.openFromRows
                val rows = listOf(
                    mapOf("_id" to 1, "name" to "Alice", "age" to 30),
                    mapOf("_id" to 2, "name" to "Bob", "age" to 25),
                    mapOf("_id" to 3, "name" to "Charlie", "age" to 35),
                )

                Relation.openFromRows(allocator, rows).use { rel ->
                    conn.bulkIngest("users", BulkIngestMode.CREATE_APPEND).use { stmt ->
                        (stmt as XtdbConnection.XtdbStatement).bind(rel)
                        val result = stmt.executeUpdate()
                        result.affectedRows shouldBe 3
                    }
                }

                // Verify the data was inserted
                conn.createStatement().use { stmt ->
                    stmt.setSqlQuery("SELECT * FROM users ORDER BY _id")
                    stmt.executeQuery().use { res ->
                        res.consumeAsMaps() shouldBe listOf(
                            mapOf("_id" to 1L, "name" to "Alice", "age" to 30L),
                            mapOf("_id" to 2L, "name" to "Bob", "age" to 25L),
                            mapOf("_id" to 3L, "name" to "Charlie", "age" to 35L),
                        )
                    }
                }
            }
        }
    }

    @Test
    fun `bulkIngest with VectorSchemaRoot binding`() {
        AdbcDriverFactory().getDriver(allocator).open(emptyMap()).use { db ->
            db.connect().use { conn ->
                // Create test data using Relation and then convert to root
                val rows = listOf(
                    mapOf("_id" to 10, "product" to "Widget", "price" to 99.99),
                    mapOf("_id" to 20, "product" to "Gadget", "price" to 149.99),
                )

                Relation.openFromRows(allocator, rows).use { rel ->
                    rel.openAsRoot(allocator).use { root ->
                        conn.bulkIngest("products", BulkIngestMode.CREATE_APPEND).use { stmt ->
                            stmt.bind(root)
                            val result = stmt.executeUpdate()
                            result.affectedRows shouldBe 2
                        }
                    }
                }

                // Verify the data was inserted
                conn.createStatement().use { stmt ->
                    stmt.setSqlQuery("SELECT * FROM products ORDER BY _id")
                    stmt.executeQuery().use { res ->
                        res.consumeAsMaps() shouldBe listOf(
                            mapOf("_id" to 10L, "product" to "Widget", "price" to 99.99),
                            mapOf("_id" to 20L, "product" to "Gadget", "price" to 149.99),
                        )
                    }
                }
            }
        }
    }

    @Test
    fun `bulkIngest with schema prefix in table name`() {
        AdbcDriverFactory().getDriver(allocator).open(emptyMap()).use { db ->
            db.connect().use { conn ->
                val rows = listOf(
                    mapOf("_id" to 1, "status" to "active"),
                    mapOf("_id" to 2, "status" to "inactive"),
                )

                Relation.openFromRows(allocator, rows).use { rel ->
                    conn.bulkIngest("custom_schema.statuses", BulkIngestMode.CREATE_APPEND).use { stmt ->
                        (stmt as XtdbConnection.XtdbStatement).bind(rel)
                        val result = stmt.executeUpdate()
                        result.affectedRows shouldBe 2
                    }
                }

                // Verify the data was inserted in the correct schema
                conn.createStatement().use { stmt ->
                    stmt.setSqlQuery("SELECT * FROM custom_schema.statuses ORDER BY _id")
                    stmt.executeQuery().use { res ->
                        res.consumeAsMaps() shouldBe listOf(
                            mapOf("_id" to 1L, "status" to "active"),
                            mapOf("_id" to 2L, "status" to "inactive"),
                        )
                    }
                }
            }
        }
    }

    @Test
    fun `bulkIngest with empty relation returns zero affected rows`() {
        AdbcDriverFactory().getDriver(allocator).open(emptyMap()).use { db ->
            db.connect().use { conn ->
                Relation(allocator, "_id" ofType UUID).use { rel ->
                    conn.bulkIngest("empty_table", BulkIngestMode.CREATE_APPEND).use { stmt ->
                        (stmt as XtdbConnection.XtdbStatement).bind(rel)
                        val result = stmt.executeUpdate()
                        result.affectedRows shouldBe 0
                    }
                }
            }
        }
    }

    @Test
    fun `bulkIngest with multiple types`() {
        AdbcDriverFactory().getDriver(allocator).open(emptyMap()).use { db ->
            db.connect().use { conn ->
                val rows = listOf(
                    mapOf(
                        "_id" to 1,
                        "name" to "Test",
                        "count" to 42,
                        "active" to true,
                        "score" to 95.5
                    ),
                    mapOf(
                        "_id" to 2,
                        "name" to "Another",
                        "count" to 13,
                        "active" to false,
                        "score" to 87.3
                    ),
                )

                Relation.openFromRows(allocator, rows).use { rel ->
                    conn.bulkIngest("mixed_types", BulkIngestMode.CREATE_APPEND).use { stmt ->
                        (stmt as XtdbConnection.XtdbStatement).bind(rel)
                        val result = stmt.executeUpdate()
                        result.affectedRows shouldBe 2
                    }
                }

                // Verify all types were preserved
                conn.createStatement().use { stmt ->
                    stmt.setSqlQuery("SELECT * FROM mixed_types ORDER BY _id")
                    stmt.executeQuery().use { res ->
                        res.consumeAsMaps() shouldBe listOf(
                            mapOf(
                                "_id" to 1L,
                                "name" to "Test",
                                "count" to 42L,
                                "active" to true,
                                "score" to 95.5
                            ),
                            mapOf(
                                "_id" to 2L,
                                "name" to "Another",
                                "count" to 13L,
                                "active" to false,
                                "score" to 87.3
                            ),
                        )
                    }
                }
            }
        }
    }
}