package xtdb.api.log

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.kafka.ConfluentKafkaContainer
import xtdb.api.log.Log.SubscriptionListener
import xtdb.api.log.Log.TailSpec
import java.time.Duration
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaConsumerGroupKeepaliveTest {

    companion object {
        // A one-second coordinator sweep, so a group left memberless becomes a deletion candidate
        // straight away instead of up to `offsets.retention.check.interval.ms` (10 min) later. Its
        // own container: KafkaClusterTest's shared one must keep the broker defaults.
        private val container =
            ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
                .withEnv("KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS", "1000")

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

    private class TermCapturingListener : SubscriptionListener<SourceMessage> {
        val term = CompletableDeferred<Long>()
        val leading = CompletableDeferred<Unit>()

        override fun launchTransition(partition: Int, termId: Long) =
            CompletableDeferred(Unit).also { term.complete(termId) }

        override fun commitLeader(partition: Int) =
            TailSpec<SourceMessage>(-1L) { }.also { leading.complete(Unit) }

        override suspend fun demoteLeader(partition: Int) {}
    }

    /**
     * Leads the topic until the keepalive offset has been committed, then leaves the group,
     * returning the term it was elected under.
     */
    private suspend fun leadThenLeave(groupId: String, topic: String): Long =
        KafkaCluster.ClusterFactory(container.bootstrapServers)
            .pollDuration(Duration.ofMillis(100))
            .groupId(groupId)
            .open()
            .use { cluster ->
                KafkaCluster.LogFactory("c", topic).openSourceLog(mapOf("c" to cluster)).use { log ->
                    coroutineScope {
                        val listener = TermCapturingListener()
                        val job = launch { log.openGroupSubscription(listener) }
                        try {
                            // Leadership has to be committed, not merely claimed: committing is what
                            // seeks the partition, so only then does the consumer have a position to
                            // commit at all.
                            val term = withTimeout(30.seconds) {
                                listener.term.await().also { listener.leading.await() }
                            }

                            // Best-effort — the keepalive has to land before we leave or there is
                            // nothing holding the group open, but a keepalive that never arrives is
                            // for the assertion below to report, not for a timeout in here.
                            withTimeoutOrNull(20.seconds) {
                                while (committedPartitions(groupId).isEmpty()) delay(500.milliseconds)
                            }

                            term
                        } finally {
                            job.cancelAndJoin()
                        }
                    }
                }
            }

    private fun committedPartitions(groupId: String) =
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to container.bootstrapServers))
            .use { admin ->
                admin.listConsumerGroupOffsets(mapOf(groupId to ListConsumerGroupOffsetsSpec()))
                    .partitionsToOffsetAndMetadata(groupId).get()
            }

    @Test
    fun `the election counter does not regress after every member leaves the group`() = runBlocking {
        val groupId = "keepalive-${UUID.randomUUID()}"
        val topic = "keepalive-${UUID.randomUUID()}"

        val firstTerm = leadThenLeave(groupId, topic)

        // Real time, not runTest's virtual clock — the broker's sweep has to actually run for the
        // absence of a deletion to mean anything.
        delay(5.seconds)

        val secondTerm = leadThenLeave(groupId, topic)

        assertTrue(
            LeaderTerm.electionOf(secondTerm) > LeaderTerm.electionOf(firstTerm),
            "election counter went from ${LeaderTerm.format(firstTerm)} to ${LeaderTerm.format(secondTerm)}" +
                    " — the group was recreated rather than surviving being memberless"
        )
    }
}
