package xtdb.api.log

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log.*
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.log.proto.TrieDetails
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.time.Duration
import java.util.Collections.synchronizedList
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaClusterTest {
    companion object {
        private val container = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            container.start()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            container.stop()
        }
    }

    @Test
    fun `round-trips messages`() = runTest(timeout = 10.seconds) {
        val msgs = synchronizedList(mutableListOf<List<Record>>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(capture(msgs)) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        fun trieDetails(key: String, size: Long) =
            TrieDetails.newBuilder()
                .setTableName("my-table").setTrieKey(key)
                .setDataFileSize(size)
                .build()

        val addedTrieDetails = listOf(trieDetails("foo", 12), trieDetails("bar", 18))

        val databaseConfig = Database.Config(
            Log.localLog("log-path".asPath), Storage.local("storage-path".asPath)
        )
        
        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .open().use { cluster ->
                KafkaCluster.LogFactory("my-cluster", "test-topic")
                    .openLog(mapOf("my-cluster" to cluster))
                    .use { log ->
                        log.subscribe(subscriber, 0).use { _ ->
                            val txPayload = ByteBuffer.allocate(9).put(-1).putLong(42).flip().array()
                            log.appendMessage(Message.Tx(txPayload)).await()

                            log.appendMessage(Message.FlushBlock(12)).await()

                            log.appendMessage(Message.TriesAdded(Storage.VERSION, addedTrieDetails)).await()

                            log.appendMessage(Message.AttachDatabase("foo", databaseConfig))

                            while (msgs.flatten().size < 4) delay(100)
                        }
                    }
            }

        assertEquals(4, msgs.flatten().size)

        val allMsgs = msgs.flatten()

        allMsgs[0].message.let {
            check(it is Message.Tx)
            assertEquals(42, ByteBuffer.wrap(it.payload).getLong(1))
        }

        allMsgs[1].message.let {
            check(it is Message.FlushBlock)
            assertEquals(12, it.expectedBlockIdx)
        }

        allMsgs[2].message.let {
            check(it is Message.TriesAdded)
            assertEquals(addedTrieDetails, it.tries)
        }

        allMsgs[3].message.let {
            check(it is Message.AttachDatabase)
            assertEquals("foo", it.dbName)
            assertEquals(databaseConfig, it.config)
        }
    }
}
