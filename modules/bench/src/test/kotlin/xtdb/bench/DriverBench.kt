package xtdb.bench

import clojure.java.api.Clojure
import clojure.lang.IFn
import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.adbc.XtdbDriver
import xtdb.api.FlightSqlServer
import xtdb.api.Xtdb
import xtdb.api.flightSqlServer
import xtdb.api.module
import java.sql.Connection
import java.sql.DriverManager
import kotlin.system.measureNanoTime

/**
 * TPC-H Benchmark comparing three access methods:
 * 1. In-Process ADBC (direct Arrow access, zero TCP overhead)
 * 2. FlightSQL/ADBC over TCP (Arrow over network with serialization)
 * 3. JDBC over PgWire (traditional row-oriented protocol)
 *
 * This demonstrates the performance characteristics of:
 * - Zero-copy in-process Arrow access (ADBC)
 * - Columnar network protocol (FlightSQL)
 * - Row-oriented network protocol (PgWire/JDBC)
 */
class DriverBench {

    private lateinit var xtdb: Xtdb
    private lateinit var allocator: BufferAllocator

    // ADBC connections
    private lateinit var adbcDb: AdbcDatabase
    private lateinit var adbcConn: AdbcConnection
    private lateinit var flightDb: AdbcDatabase
    private lateinit var flightConn: AdbcConnection

    // JDBC connection (PgWire)
    private lateinit var jdbcConn: Connection

    private var flightSqlPort: Int = 0
    private var pgwirePort: Int = 0

    companion object {
        // Simple TPC-H queries that work well for comparison
        val QUERIES = mapOf(
            "Q6" to """
                SELECT SUM(l_extendedprice * l_discount) AS revenue
                FROM lineitem
                WHERE l_shipdate >= DATE '1994-01-01'
                  AND l_shipdate < DATE '1995-01-01'
                  AND l_discount BETWEEN 0.05 AND 0.07
                  AND l_quantity < 24
            """.trimIndent(),

            "Q1-simple" to """
                SELECT
                    l_returnflag,
                    l_linestatus,
                    COUNT(*) AS count_order
                FROM lineitem
                WHERE l_shipdate <= DATE '1998-09-02'
                GROUP BY l_returnflag, l_linestatus
                ORDER BY l_returnflag, l_linestatus
            """.trimIndent(),

            "Point-Query" to """
                SELECT * FROM customer WHERE c_custkey = 1
            """.trimIndent(),

            "Small-Agg" to """
                SELECT COUNT(*) AS cnt FROM orders WHERE o_orderdate >= DATE '1995-01-01'
            """.trimIndent(),

            "Wide-Projection" to """
                SELECT * FROM lineitem LIMIT 100
            """.trimIndent(),

            "Medium-Result" to """
                SELECT * FROM lineitem LIMIT 1000
            """.trimIndent(),

            "Large-Result" to """
                SELECT * FROM orders
            """.trimIndent(),

            "Join-Query" to """
                SELECT o.o_orderkey, o.o_orderdate, c.c_name
                FROM orders o
                JOIN customer c ON o.o_custkey = c.c_custkey
                LIMIT 500
            """.trimIndent()
        )

        const val WARMUP_ITERATIONS = 3
        const val BENCHMARK_ITERATIONS = 10
    }

    @BeforeEach
    fun setUp() {
        println("\n=== Setting up TPC-H Benchmark ===")

        // Start XTDB node with FlightSQL server
        xtdb = Xtdb.openNode {
            flightSqlServer { port = 0 }
        }

        // Get FlightSQL port
        flightSqlPort = xtdb.module<FlightSqlServer>()!!.port
        println("FlightSQL server started on port: $flightSqlPort")

        // Get PgWire port
        pgwirePort = xtdb.serverPort
        println("PgWire server started on port: $pgwirePort")

        // Load TPC-H data (scale factor 0.01)
        loadTpchData(0.01)

        // Setup allocator
        allocator = RootAllocator()

        // Setup In-Process ADBC
        adbcDb = XtdbDriver(allocator).open(mutableMapOf(
            XtdbDriver.PARAM_XTDB_NODE to xtdb
        ))
        adbcConn = adbcDb.connect()
        println("✓ In-Process ADBC connected")

        // Setup FlightSQL ADBC
        flightDb = FlightSqlDriver(allocator).open(mapOf(
            "uri" to "grpc+tcp://127.0.0.1:$flightSqlPort"
        ))
        flightConn = flightDb.connect()
        println("✓ FlightSQL ADBC connected")

        // Setup JDBC (PgWire)
        jdbcConn = DriverManager.getConnection(
            "jdbc:xtdb://localhost:$pgwirePort/xtdb",
            "xtdb",
            "xtdb"
        )
        println("✓ JDBC (PgWire) connected")

        println("=== Setup Complete ===\n")
    }

    @AfterEach
    fun tearDown() {
        jdbcConn.close()
        flightConn.close()
        flightDb.close()
        adbcConn.close()
        adbcDb.close()
        allocator.close()
        xtdb.close()
    }

    private fun loadTpchData(scaleFactor: Double) {
        println("Loading TPC-H data (scale factor: $scaleFactor)...")

        val require = Clojure.`var`("clojure.core", "require")
        require.invoke(Clojure.read("xtdb.datasets.tpch"))

        val submitDocs: IFn = Clojure.`var`("xtdb.datasets.tpch", "submit-docs!")
        submitDocs.invoke(xtdb, scaleFactor)

        // Wait for indexing
        val sync = Clojure.`var`("xtdb.log", "sync-node")
        sync.invoke(xtdb)

        println("✓ TPC-H data loaded and indexed")
    }

    data class QueryResult(
        val queryName: String,
        val method: String,
        val avgTimeMs: Double,
        val minTimeMs: Double,
        val maxTimeMs: Double,
        val rowCount: Long,
        val iterations: Int
    )

    private fun benchmarkQuery(
        queryName: String,
        sql: String,
        iterations: Int = BENCHMARK_ITERATIONS
    ): List<QueryResult> {
        println("\n--- Benchmarking: $queryName ---")

        val results = mutableListOf<QueryResult>()

        // 1. In-Process ADBC
        val adbcTimes = mutableListOf<Long>()
        var adbcRowCount = 0L

        repeat(WARMUP_ITERATIONS) {
            adbcConn.createStatement().use { stmt ->
                stmt.setSqlQuery(sql)
                stmt.executeQuery().reader.use { reader ->
                    while (reader.loadNextBatch()) { /* consume */ }
                }
            }
        }

        repeat(iterations) {
            val time = measureNanoTime {
                adbcConn.createStatement().use { stmt ->
                    stmt.setSqlQuery(sql)
                    stmt.executeQuery().reader.use { reader ->
                        var count = 0L
                        while (reader.loadNextBatch()) {
                            count += reader.vectorSchemaRoot.rowCount
                        }
                        adbcRowCount = count
                    }
                }
            }
            adbcTimes.add(time)
        }

        results.add(QueryResult(
            queryName = queryName,
            method = "In-Process ADBC",
            avgTimeMs = adbcTimes.average() / 1_000_000.0,
            minTimeMs = adbcTimes.minOrNull()!! / 1_000_000.0,
            maxTimeMs = adbcTimes.maxOrNull()!! / 1_000_000.0,
            rowCount = adbcRowCount,
            iterations = iterations
        ))

        // 2. FlightSQL ADBC
        val flightTimes = mutableListOf<Long>()
        var flightRowCount = 0L

        repeat(WARMUP_ITERATIONS) {
            flightConn.createStatement().use { stmt ->
                stmt.setSqlQuery(sql)
                stmt.executeQuery().reader.use { reader ->
                    while (reader.loadNextBatch()) { /* consume */ }
                }
            }
        }

        repeat(iterations) {
            val time = measureNanoTime {
                flightConn.createStatement().use { stmt ->
                    stmt.setSqlQuery(sql)
                    stmt.executeQuery().reader.use { reader ->
                        var count = 0L
                        while (reader.loadNextBatch()) {
                            count += reader.vectorSchemaRoot.rowCount
                        }
                        flightRowCount = count
                    }
                }
            }
            flightTimes.add(time)
        }

        results.add(QueryResult(
            queryName = queryName,
            method = "FlightSQL/TCP",
            avgTimeMs = flightTimes.average() / 1_000_000.0,
            minTimeMs = flightTimes.minOrNull()!! / 1_000_000.0,
            maxTimeMs = flightTimes.maxOrNull()!! / 1_000_000.0,
            rowCount = flightRowCount,
            iterations = iterations
        ))

        // 3. JDBC (PgWire)
        val jdbcTimes = mutableListOf<Long>()
        var jdbcRowCount = 0L

        repeat(WARMUP_ITERATIONS) {
            jdbcConn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    while (rs.next()) { /* consume */ }
                }
            }
        }

        repeat(iterations) {
            val time = measureNanoTime {
                jdbcConn.createStatement().use { stmt ->
                    stmt.executeQuery(sql).use { rs ->
                        var count = 0L
                        while (rs.next()) {
                            count++
                        }
                        jdbcRowCount = count
                    }
                }
            }
            jdbcTimes.add(time)
        }

        results.add(QueryResult(
            queryName = queryName,
            method = "JDBC/PgWire",
            avgTimeMs = jdbcTimes.average() / 1_000_000.0,
            minTimeMs = jdbcTimes.minOrNull()!! / 1_000_000.0,
            maxTimeMs = jdbcTimes.maxOrNull()!! / 1_000_000.0,
            rowCount = jdbcRowCount,
            iterations = iterations
        ))

        return results
    }

    private fun printResults(allResults: List<QueryResult>) {
        println("\n" + "=".repeat(100))
        println("TPC-H Benchmark Results (Scale Factor: 0.01)")
        println("=".repeat(100))
        println()

        val grouped = allResults.groupBy { it.queryName }

        for ((queryName, results) in grouped) {
            println("Query: $queryName (${results[0].rowCount} rows)")
            println("-".repeat(100))
            println(String.format("%-20s | %12s | %12s | %12s | %10s",
                "Method", "Avg (ms)", "Min (ms)", "Max (ms)", "Speedup"))
            println("-".repeat(100))

            val baseline = results.find { it.method == "JDBC/PgWire" }?.avgTimeMs ?: 1.0

            for (result in results) {
                val speedup = baseline / result.avgTimeMs
                println(String.format("%-20s | %12.3f | %12.3f | %12.3f | %10.2fx",
                    result.method,
                    result.avgTimeMs,
                    result.minTimeMs,
                    result.maxTimeMs,
                    speedup
                ))
            }
            println()
        }

        // Summary statistics
        println("=".repeat(100))
        println("Summary")
        println("=".repeat(100))

        val adbcResults = allResults.filter { it.method == "In-Process ADBC" }
        val flightResults = allResults.filter { it.method == "FlightSQL/TCP" }
        val jdbcResults = allResults.filter { it.method == "JDBC/PgWire" }

        println(String.format("In-Process ADBC avg: %.3f ms", adbcResults.map { it.avgTimeMs }.average()))
        println(String.format("FlightSQL/TCP avg:   %.3f ms", flightResults.map { it.avgTimeMs }.average()))
        println(String.format("JDBC/PgWire avg:     %.3f ms", jdbcResults.map { it.avgTimeMs }.average()))
        println()

        val adbcVsJdbc = jdbcResults.map { it.avgTimeMs }.average() / adbcResults.map { it.avgTimeMs }.average()
        val flightVsJdbc = jdbcResults.map { it.avgTimeMs }.average() / flightResults.map { it.avgTimeMs }.average()

        println(String.format("In-Process ADBC is %.2fx faster than JDBC/PgWire", adbcVsJdbc))
        println(String.format("FlightSQL/TCP is %.2fx faster than JDBC/PgWire", flightVsJdbc))
        println()
        println("Protocol Comparison:")
        println("  • In-Process ADBC: Zero network overhead, direct Arrow buffer access")
        println("  • FlightSQL/TCP: Columnar Arrow over network, batch-oriented")
        println("  • JDBC/PgWire: Row-oriented protocol, row-by-row iteration")
        println("=".repeat(100))
    }

    @Test
    fun `benchmark TPC-H queries`() {
        val allResults = mutableListOf<QueryResult>()

        for ((queryName, sql) in QUERIES) {
            val results = benchmarkQuery(queryName, sql)
            allResults.addAll(results)
        }

        printResults(allResults)
    }
}
