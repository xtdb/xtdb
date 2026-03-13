package xtdb.debezium

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import xtdb.api.log.Log
import xtdb.api.log.Log.*
import xtdb.api.log.MessageId
import xtdb.api.log.SourceMessage
import xtdb.util.MsgIdUtil
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

private val logger = LoggerFactory.getLogger(DebeziumLog::class.java)

object UnitDeserializer : Deserializer<Unit> {
    override fun deserialize(topic: String?, data: ByteArray) = Unit
}

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

    override fun openGroupConsumer(listener: SubscriptionListener): Log.Consumer<SourceMessage> =
        throw UnsupportedOperationException("CDC log does not support group subscription")

    override fun readLastMessage(): SourceMessage? = null

    override fun openConsumer(): Log.Consumer<SourceMessage> = object : Log.Consumer<SourceMessage> {
        override fun tailAll(afterMsgId: MessageId, processor: Log.RecordProcessor<SourceMessage>): Log.Subscription {
            val afterOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)
            val ch = kotlinx.coroutines.channels.Channel<List<Log.Record<SourceMessage>>>(10)

            val producerJob = scope.launch {
                KafkaConsumer(
                    kafkaConfig + mapOf(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                    ),
                    UnitDeserializer,
                    ByteArrayDeserializer()
                ).use { c ->
                    TopicPartition(topic, 0).also { tp ->
                        c.assign(listOf(tp))
                        c.seek(tp, afterOffset + 1)
                    }
                    runInterruptible(Dispatchers.IO) {
                        while (true) {
                            val records = try {
                                c.poll(pollDuration).records(topic)
                            } catch (_: InterruptException) {
                                throw InterruptedException()
                            }

                            ch.trySend(
                                records.mapNotNull { record ->
                                    record.value()?.let { value ->
                                        Log.Record(
                                            epoch,
                                            record.offset(),
                                            Instant.ofEpochMilli(record.timestamp()),
                                            SourceMessage.Tx(value),
                                        )
                                    }
                                }
                            )
                        }
                    }
                }
            }

            val consumerJob = scope.launch {
                try {
                    while (isActive) {
                        val records = ch.receive()
                        if (records.isNotEmpty()) runInterruptible { processor.processRecords(records) }
                    }
                } finally {
                    producerJob.cancelAndJoin()
                }
            }

            return Log.Subscription { runBlocking { withTimeout(5.seconds) { consumerJob.cancelAndJoin() } } }
        }

        override fun close() {}
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
