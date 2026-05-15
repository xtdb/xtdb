package xtdb.postgres

import io.minio.MakeBucketArgs
import io.minio.MinioClient
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
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
import xtdb.api.log.ReplicaMessage
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
        // KafkaCluster.LogFactory auto-derives the replica topic as "${log}-replica".
        private const val CDC_REPLICA_TOPIC = "$CDC_SOURCE_TOPIC-replica"
        private const val CDC_DB = "cdc"

        // 1 tx per table snapshot + 1 snapshot-complete marker
        private const val SNAPSHOT_TXS = (NUM_TABLES + 1).toLong()
        private const val SNAPSHOT_ROWS = (NUM_TABLES * ROWS_PER_TABLE).toLong()

        // Time to let the late follower attempt to catch up (and the streamed tx propagate)
        // before asserting state. Bigger than typical convergence; just long enough that
        // a working follower would converge well within this window.
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

    private fun seedReplicaTopicNoOps(count: Int = 10) {
        val props = mapOf<String, Any>("bootstrap.servers" to kafka.bootstrapServers, "acks" to "all")
        KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer()).use { producer ->
            val bytes = ReplicaMessage.NoOp.encode()
            repeat(count) { producer.send(ProducerRecord(CDC_REPLICA_TOPIC, bytes)) }
            producer.flush()
        }
    }

    /**
     * Mimics a replica topic that's been written to by an earlier CDC attempt
     * whose oldest messages have since aged out via Kafka retention — low-watermark
     * advances past 0 with no block on object store anchoring follower recovery.
     */
    private fun truncateReplicaTopicPrefix(deleteBefore: Long) {
        val props = mapOf<String, Any>("bootstrap.servers" to kafka.bootstrapServers)
        AdminClient.create(props).use { admin ->
            val tp = TopicPartition(CDC_REPLICA_TOPIC, 0)
            admin.deleteRecords(mapOf(tp to RecordsToDelete.beforeOffset(deleteBefore))).all().get()
        }
        logReplicaOffsets("after truncate")
    }

    private fun logReplicaOffsets(label: String) {
        val props = mapOf<String, Any>("bootstrap.servers" to kafka.bootstrapServers)
        AdminClient.create(props).use { admin ->
            val tp = TopicPartition(CDC_REPLICA_TOPIC, 0)
            val earliest = admin.listOffsets(mapOf(tp to OffsetSpec.earliest())).all().get()[tp]?.offset()
            val latest = admin.listOffsets(mapOf(tp to OffsetSpec.latest())).all().get()[tp]?.offset()
            log.info("replica topic offsets ($label): earliest=$earliest, latest=$latest")
        }
    }

    private suspend fun settleThenAssert(
        label: String, nodes: List<Xtdb>, expectedRows: Long, expectedTxs: Long,
    ) {
        runInterruptible { Thread.sleep(SETTLE_TIME.inWholeMilliseconds) }
        nodes.forEachIndexed { i, n ->
            val rows = rowCount(n)
            val txs = txCount(n)
            log.info("$label: node${i + 1} rows=$rows (expected $expectedRows), txs=$txs (expected $expectedTxs)")
            assertEquals(expectedRows, rows, "$label: node${i + 1} row total")
            assertEquals(expectedTxs, txs, "$label: node${i + 1} tx count")
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

    /**
     * Regression test for #5618: a prior CDC attempt left messages on the replica topic and
     * retention has aged the oldest offsets out, so the partition's low-watermark sits past 0.
     * No block has been written to object store yet, so a fresh follower comes up with
     * `replica: -1` and must catch up on the snapshot from the replica log alone.
     *
     * Expected behaviour (post-fix): the late follower converges on the leader's state for
     * both the snapshot and the subsequent streamed tx.
     *
     * Pre-fix, this test will fail because the consumer seeks to offset 0 (out-of-range on a
     * truncated topic) and `auto.offset.reset=latest` skips it past the snapshot batch —
     * leaving the follower with only the streamed tx and missing all the snapshot rows.
     */
    @Test
    fun `postgres-source snapshot converges with truncated replica topic prefix`(
        @TempDir leaderDir: Path, @TempDir lateFollowerDir: Path,
    ) = runTest(timeout = 90.seconds) {
        seedPostgres()
        // Mimic an earlier CDC attempt's messages on the replica topic, with the oldest
        // offsets having aged out under retention.
        seedReplicaTopicNoOps(count = 20)
        truncateReplicaTopicPrefix(deleteBefore = 15L)

        openNode(leaderDir).use { leader ->
            attachPostgresSource(leader)
            settleThenAssert("leader after snapshot (truncated prefix)", listOf(leader), SNAPSHOT_ROWS, SNAPSHOT_TXS)
            logReplicaOffsets("after snapshot, before late follower opens")

            // Fresh follower joins post-snapshot — pod-1 shape from the #5618 logs.
            val lateFollower = openNode(lateFollowerDir)
            val testFailure = try {
                logReplicaOffsets("late follower opened")
                settleThenAssert("late follower caught up on snapshot",
                    listOf(leader, lateFollower), SNAPSHOT_ROWS, SNAPSHOT_TXS)

                // And after a streamed insert, both nodes still agree.
                pgExecute("INSERT INTO ${tableName(1)} (_id, payload) VALUES (9999, 'streamed')")
                settleThenAssert("late follower caught up after streaming",
                    listOf(leader, lateFollower), SNAPSHOT_ROWS + 1, SNAPSHOT_TXS + 1)
                null
            } catch (e: Throwable) { e }

            // Close the late follower first, then let node1's post-rebalance leader transition
            // settle before the outer .use closes it. Without this, leader.close() lands while
            // node1 is mid-rebalance and we hit a separate close-during-rebalance deadlock that
            // masks the #5618 assertion failure as an indefinite hang.
            lateFollower.close()
            runInterruptible { Thread.sleep(SETTLE_TIME.inWholeMilliseconds) }
            testFailure?.let { throw it }
        }
    }

}
