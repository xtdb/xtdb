package xtdb.debezium

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import xtdb.api.log.Log
import xtdb.api.log.Log.*
import xtdb.api.log.SourceMessage
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

private val logger = LoggerFactory.getLogger(DebeziumLog::class.java)

class DebeziumLog @JvmOverloads constructor(
    private val kafkaConfig: Map<String, String>,
    private val topic: String,
    private val pollDuration: Duration = Duration.ofSeconds(1),
    coroutineContext: CoroutineContext = Dispatchers.Default,
) : Log<SourceMessage> {

    // TODO: non-deterministic failures (e.g. node down) currently kill this coroutine silently.
    private val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
        logger.error("Fatal error in CDC ingestion — ingestion has stopped", throwable)
    }

    private val scope = CoroutineScope(SupervisorJob() + coroutineContext + exceptionHandler)

    override val latestSubmittedOffset: Long get() = -1
    override val epoch: Int get() = 0

    override fun appendMessage(message: SourceMessage): CompletableFuture<MessageMetadata> =
        throw UnsupportedOperationException("CDC log is read-only")

    override fun openAtomicProducer(transactionalId: String): AtomicProducer<SourceMessage> =
        throw UnsupportedOperationException("CDC log is read-only")

    override fun subscribe(subscriber: GroupSubscriber<SourceMessage>): Subscription =
        throw UnsupportedOperationException("CDC log does not support group subscription")

    override fun readLastMessage(): SourceMessage? = null

    override fun tailAll(subscriber: Subscriber<SourceMessage>, latestProcessedOffset: Long): Subscription {
        val job = scope.launch {
            KafkaConsumer<String, String>(
                kafkaConfig + mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                )
            ).use { c ->
                TopicPartition(topic, 0).also { tp ->
                    c.assign(listOf(tp))
                    c.seek(tp, latestProcessedOffset + 1)
                }
                runInterruptible(Dispatchers.IO) {
                    while (true) {
                        val records = try {
                            c.poll(pollDuration).records(topic)
                        } catch (_: InterruptException) {
                            throw InterruptedException()
                        }

                        subscriber.processRecords(
                            records.mapNotNull { record ->
                                record.value()?.let { value ->
                                    Record(
                                        record.offset(),
                                        Instant.ofEpochMilli(record.timestamp()),
                                        SourceMessage.Tx(value.toByteArray()),
                                    )
                                }
                            }
                        )
                    }
                }
            }
        }

        return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
