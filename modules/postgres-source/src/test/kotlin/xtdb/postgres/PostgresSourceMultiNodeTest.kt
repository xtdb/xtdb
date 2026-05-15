package xtdb.postgres

import io.minio.MakeBucketArgs
import io.minio.MinioClient
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.containers.Network
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import software.amazon.awssdk.regions.Region.AWS_ISO_GLOBAL
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import xtdb.api.storage.Storage
import xtdb.aws.S3
import xtdb.cache.DiskCache
import xtdb.util.info
import xtdb.util.logger
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.io.path.Path
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class PostgresSourceMultiNodeTest {

    companion object {
        private val log = PostgresSourceMultiNodeTest::class.logger

        private const val NUM_TABLES = 10
        private const val ROWS_PER_TABLE = 100
        private const val MINIO_BUCKET = "xtdb-test"
        private const val SOURCE_TOPIC = "test-src"
        private const val CDC_SOURCE_TOPIC = "test-cdc"
        private const val CDC_DB = "cdc"

        // 1 tx per table snapshot + 1 snapshot-complete marker
        private const val SNAPSHOT_TXS = (NUM_TABLES + 1).toLong()
        private const val SNAPSHOT_ROWS = (NUM_TABLES * ROWS_PER_TABLE).toLong()

        // Time to let nodes converge on the replica log before asserting state.
        // Comfortably longer than typical convergence under happy-path conditions.
        private val SETTLE_TIME = 5.seconds

        private fun tableName(idx: Int) = "pg_mn_t${"%02d".format(idx)}"
    }

    private lateinit var network: Network
    private lateinit var postgres: PostgreSQLContainer
    private lateinit var kafka: ConfluentKafkaContainer
    private lateinit var minio: MinIOContainer

    @BeforeEach
    fun beforeEach() {
        log.info("beforeEach: starting postgres, kafka, minio containers")
        network = Network.newNetwork()

        postgres = PostgreSQLContainer("postgres:17-alpine")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCommand("postgres", "-c", "wal_level=logical")

        kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")

        minio = MinIOContainer("minio/minio:RELEASE.2025-09-07T16-13-09Z")
            .withNetwork(network)
            .withNetworkAliases("minio")

        Startables.deepStart(postgres, kafka, minio).join()

        MinioClient.builder()
            .endpoint(minio.s3URL).credentials(minio.userName, minio.password)
            .build()
            .makeBucket(MakeBucketArgs.builder().bucket(MINIO_BUCKET).build())
        log.info("beforeEach: containers ready")
    }

    @AfterEach
    fun afterEach() {
        log.info("afterEach: stopping containers")
        postgres.stop()
        kafka.stop()
        minio.stop()
        network.close()
        log.info("afterEach: done")
    }

    private fun seedPostgres() {
        val stmts = (1..NUM_TABLES).flatMap { i ->
            val name = tableName(i)
            listOf("CREATE TABLE IF NOT EXISTS $name (_id INT PRIMARY KEY, payload TEXT)") +
                (1..ROWS_PER_TABLE).map { r -> "INSERT INTO $name (_id, payload) VALUES ($r, 'row-$r')" }
        }
        val tableList = (1..NUM_TABLES).joinToString(", ") { tableName(it) }
        pgExecute(*stmts.toTypedArray(), "CREATE PUBLICATION test_pub FOR TABLE $tableList")
    }

    private fun pgExecute(vararg statements: String) {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt -> for (sql in statements) stmt.execute(sql) }
        }
    }

    private fun openNode(nodeDir: Path): Xtdb = Xtdb.openNode {
        server { port = 0 }; flightSql = null
        diskCache(DiskCache.factory(nodeDir.resolve("disk-cache")))
        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
        remote("pg", PostgresRemote.Factory(
            hostname = postgres.host,
            port = postgres.getMappedPort(5432),
            database = "testdb",
            username = "testuser",
            password = "testpass",
        ))
        log(KafkaCluster.LogFactory("kafka", SOURCE_TOPIC))
        storage(Storage.remote(S3.s3(MINIO_BUCKET) {
            endpoint(minio.s3URL)
            credentials(minio.userName, minio.password)
            region(AWS_ISO_GLOBAL.id())
            prefix(Path("primary"))
            pathStyleAccessEnabled(true)
        }))
    }

    private fun attachPostgresSource(node: Xtdb) {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("""
                    ATTACH DATABASE $CDC_DB WITH $$
                        log: !Kafka
                          cluster: kafka
                          topic: $CDC_SOURCE_TOPIC
                        storage: !Remote
                          objectStore: !S3
                            bucket: "$MINIO_BUCKET"
                            endpoint: "${minio.s3URL}"
                            region: "${AWS_ISO_GLOBAL.id()}"
                            credentials:
                              accessKey: "${minio.userName}"
                              secretKey: "${minio.password}"
                            pathStyleAccessEnabled: true
                            prefix: "cdc"
                        externalSource: !Postgres
                          remote: pg
                          slotName: test_slot
                          publicationName: test_pub
                          schemaIncludeList: [public]
                    $$""".trimIndent())
            }
        }
    }

    private fun scalarLong(node: Xtdb, sql: String): Long =
        node.createConnectionBuilder().database(CDC_DB).build().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs -> rs.next(); rs.getLong(1) }
            }
        }

    private fun rowCount(node: Xtdb): Long =
        (1..NUM_TABLES).sumOf { scalarLong(node, "SELECT count(*) FROM public.${tableName(it)}") }

    private fun txCount(node: Xtdb): Long =
        scalarLong(node, "SELECT count(*) FROM xt.txs FOR ALL VALID_TIME")

    private suspend fun settleThenAssert(
        label: String, nodes: List<Xtdb>, expectedRows: Long, expectedTxs: Long,
    ) {
        runInterruptible { Thread.sleep(SETTLE_TIME.inWholeMilliseconds) }
        nodes.forEachIndexed { i, n ->
            assertEquals(expectedRows, rowCount(n), "$label: node${i + 1} row total")
            assertEquals(expectedTxs, txCount(n), "$label: node${i + 1} tx count")
        }
    }

    /**
     * Baseline 3-node postgres-source flow. Establishes the happy path each subsequent test
     * deviates from, and asserts the prod-shape "late follower joins post-snapshot, pre-stream"
     * scenario from #5618 against a clean (non-truncated) replica topic.
     */
    @Test
    fun `postgres-source full lifecycle - snapshot, late joiners, streaming`(
        @TempDir node1Dir: Path, @TempDir node2Dir: Path, @TempDir node3Dir: Path,
        @TempDir preStreamLateDir: Path, @TempDir postStreamLateDir: Path,
    ) = runTest(timeout = 120.seconds) {
        seedPostgres()

        openNode(node1Dir).use { node1 ->
            openNode(node2Dir).use { node2 ->
                openNode(node3Dir).use { node3 ->
                    val nodes = listOf(node1, node2, node3)
                    attachPostgresSource(node1)

                    // Leader takes the snapshot; followers catch up via the replica log.
                    settleThenAssert("after snapshot", nodes, SNAPSHOT_ROWS, SNAPSHOT_TXS)

                    // Pod-1 prod shape from #5618: a fresh follower joins after snapshot,
                    // before any streaming tx has arrived.
                    openNode(preStreamLateDir).use { preStreamLate ->
                        settleThenAssert("pre-streaming late joiner caught up",
                            nodes + preStreamLate, SNAPSHOT_ROWS, SNAPSHOT_TXS)
                    }

                    // One real streamed tx, then a final late joiner — both should converge.
                    pgExecute("INSERT INTO ${tableName(1)} (_id, payload) VALUES (9999, 'streamed')")
                    settleThenAssert("after streaming", nodes, SNAPSHOT_ROWS + 1, SNAPSHOT_TXS + 1)

                    openNode(postStreamLateDir).use { postStreamLate ->
                        settleThenAssert("post-streaming late joiner caught up",
                            nodes + postStreamLate, SNAPSHOT_ROWS + 1, SNAPSHOT_TXS + 1)
                    }
                }
            }
        }
    }

}
