package xtdb.debezium

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import xtdb.api.log.Log
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

private val logger = LoggerFactory.getLogger(DebeziumConsumer::class.java)

object UnitDeserializer : Deserializer<Unit> {
    override fun deserialize(topic: String?, data: ByteArray) = Unit
}

class DebeziumMessage(
    val payload: ByteArray,
    val offsets: Map<TopicPartition, OffsetAndMetadata>,
    val consumerGroupMetadata: ConsumerGroupMetadata,
)

class DebeziumConsumer @JvmOverloads constructor(
    private val kafkaConfig: Map<String, String>,
    private val topic: String,
    private val groupId: String,
    private val pollDuration: Duration = Duration.ofSeconds(1),
    coroutineContext: CoroutineContext = Dispatchers.Default,
) : AutoCloseable {

    // TODO: non-deterministic failures (e.g. node down) currently kill this coroutine silently.
    private val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
        logger.error("Fatal error in CDC ingestion — ingestion has stopped", throwable)
    }

    private val scope = CoroutineScope(SupervisorJob() + coroutineContext + exceptionHandler)
    val epoch: Int get() = 0

    fun tailAll(processor: Log.RecordProcessor<DebeziumMessage>): Log.Subscription {
        val job = scope.launch {
            KafkaConsumer(
                kafkaConfig.plus(mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                    ConsumerConfig.GROUP_ID_CONFIG to groupId,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
                )),
                UnitDeserializer,
                ByteArrayDeserializer()
            ).use { c ->
                c.subscribe(listOf(topic))
                while (isActive) {
                    val records = runInterruptible(Dispatchers.IO) {
                        try {
                            c.poll(pollDuration).records(topic).mapNotNull { record ->
                                record.value()?.let { value ->
                                    val tp = TopicPartition(record.topic(), record.partition())
                                    Log.Record(
                                        epoch,
                                        record.offset(),
                                        Instant.ofEpochMilli(record.timestamp()),
                                        DebeziumMessage(
                                            value,
                                            mapOf(tp to OffsetAndMetadata(record.offset() + 1)),
                                            // TODO: groupMetadata captured at poll-time may become stale if a rebalance
                                            //  occurs before sendOffsetsToTransaction — would cause ProducerFencedException.
                                            //  Acceptable for single-consumer setups; no data loss (transaction aborts).
                                            c.groupMetadata()
                                        ),
                                    )
                                }
                            }
                        } catch (_: InterruptException) {
                            throw InterruptedException()
                        }
                    }

                    processor.processRecords(records)
                }
            }
        }

        return Log.Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
