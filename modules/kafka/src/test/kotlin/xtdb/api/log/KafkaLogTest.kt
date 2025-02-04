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
import xtdb.api.log.Log.Message
import xtdb.api.log.Log.Record
import xtdb.api.log.Log.Subscriber
import xtdb.log.proto.AddedTrie
import java.nio.ByteBuffer
import java.time.Duration
import java.util.Collections.synchronizedList
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaLogTest {
    companion object {
        private val container = ConfluentKafkaContainer("confluentinc/cp-kafka:latest")

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
            every { latestCompletedOffset } returns -1
        }

        fun addedTrie(key: String, size: Long) =
            AddedTrie.newBuilder()
                .setTableName("my-table").setTrieKey(key)
                .setDataFileSize(size)
                .build()

        val addedTries = listOf(addedTrie("foo", 12), addedTrie("bar", 18))

        KafkaLog.kafka(container.bootstrapServers, "test-topic")
            .pollDuration(Duration.ofMillis(100))
            .openLog().use { log ->
                log.subscribe(subscriber).use { _ ->
                    val txPayload = ByteBuffer.allocate(9).put(-1).putLong(42).flip()
                    log.appendMessage(Message.Tx(txPayload)).await()

                    log.appendMessage(Message.FlushBlock(12)).await()

                    log.appendMessage(Message.TriesAdded(addedTries)).await()

                    while (msgs.flatten().size < 3) delay(100)
                }
            }

        assertEquals(3, msgs.flatten().size)

        val allMsgs = msgs.flatten()

        allMsgs[0].message.let {
            check(it is Message.Tx)
            assertEquals(42, it.payload.getLong(1))
        }

        allMsgs[1].message.let {
            check(it is Message.FlushBlock)
            assertEquals(12, it.expectedBlockTxId)
        }

        allMsgs[2].message.let {
            check(it is Message.TriesAdded)
            assertEquals(addedTries, it.tries)
        }
    }
}
