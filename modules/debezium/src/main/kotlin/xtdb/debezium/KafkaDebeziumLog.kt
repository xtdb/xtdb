package xtdb.debezium

import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import xtdb.api.log.KafkaCluster
import xtdb.api.log.Log
import xtdb.api.log.ensureTopicExists
import xtdb.api.log.LogClusterAlias
import xtdb.database.ExternalLog
import xtdb.database.ExternalLog.MessageProcessor
import xtdb.debezium.proto.DebeziumOffsetToken
import xtdb.debezium.proto.KafkaDebeziumLogConfig
import xtdb.debezium.proto.debeziumOffsetToken
import xtdb.debezium.proto.kafkaDebeziumLogConfig
import xtdb.debezium.proto.partitionOffsets
import xtdb.database.ExternalSourceToken
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

private val LOG = LoggerFactory.getLogger(KafkaDebeziumLog::class.java)

@OptIn(ExperimentalCoroutinesApi::class)
class KafkaDebeziumLog @JvmOverloads constructor(
    private val kafkaConfig: Map<String, String>,
    private val topic: String,
    private val groupId: String,
    private val pollDuration: Duration = Duration.ofSeconds(1),
    coroutineContext: CoroutineContext = Dispatchers.Default,
) : DebeziumLog {

    object UnitDeserializer : Deserializer<Unit> {
        override fun deserialize(topic: String?, data: ByteArray) = Unit
    }

    @Serializable
    @SerialName("!Kafka")
    data class Factory(
        val logCluster: LogClusterAlias, val tableTopic: String, val groupId: String
    ) : DebeziumLog.Factory {

        override fun openLog(clusters: Map<LogClusterAlias, Log.Cluster>): DebeziumLog {
            val cluster =
                requireNotNull(clusters[logCluster] as? KafkaCluster) { "missing Kafka cluster: '${logCluster}'" }

            AdminClient.create(cluster.kafkaConfigMap).use { admin ->
                admin.ensureTopicExists(tableTopic, autoCreate = false)
            }

            return KafkaDebeziumLog(cluster.kafkaConfigMap, tableTopic, groupId)
        }

        fun toProto(): KafkaDebeziumLogConfig = kafkaDebeziumLogConfig {
            this.logCluster = this@Factory.logCluster
            this.tableTopic = this@Factory.tableTopic
            this.groupId = this@Factory.groupId
        }

        companion object {
            fun fromProto(proto: KafkaDebeziumLogConfig) =
                Factory(
                    logCluster = proto.logCluster,
                    tableTopic = proto.tableTopic,
                    groupId = proto.groupId,
                )
        }
    }

    private val _error = CompletableDeferred<Throwable>()
    val error: Throwable? get() = if (_error.isCompleted) _error.getCompleted() else null
    suspend fun awaitError(): Throwable = _error.await()

    // TODO: non-deterministic failures (e.g. node down) currently kill this coroutine silently.
    private val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
        LOG.error("Fatal error in CDC ingestion — ingestion has stopped", throwable)
        _error.complete(throwable)
    }

    private val scope = CoroutineScope(SupervisorJob() + coroutineContext + exceptionHandler)
    val epoch: Int get() = 0

    override fun tailAll(afterToken: ExternalSourceToken?, processor: MessageProcessor<DebeziumMessage>): Log.Subscription {
        val job = scope.launch {
            KafkaConsumer(
                kafkaConfig.plus(
                    mapOf(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                        ConsumerConfig.GROUP_ID_CONFIG to groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
                    )
                ),
                UnitDeserializer,
                ByteArrayDeserializer()
            ).use { c ->
                val partitionOffsets = afterToken?.let { tok ->
                    val token = tok.unpack(DebeziumOffsetToken::class.java)
                    token.dbzmTopicOffsetsMap[topic]?.offsetsList
                        ?.mapIndexedNotNull { partition, offset ->
                            if (offset >= 0) TopicPartition(topic, partition) to offset else null
                        }
                        ?.takeIf { it.isNotEmpty() }
                }

                if (partitionOffsets != null) {
                    c.assign(partitionOffsets.map { it.first })
                    partitionOffsets.forEach { (tp, offset) -> c.seek(tp, offset + 1) }
                } else {
                    c.subscribe(listOf(topic))
                }

                while (isActive) {
                    val records = runInterruptible(Dispatchers.IO) {
                        try {
                            c.poll(pollDuration).records(topic).mapNotNull { record ->
                                record.value()?.let { value ->
                                    val offsets = debeziumOffsetToken {
                                        dbzmTopicOffsets[record.topic()] = partitionOffsets {
                                            val arr = LongArray(record.partition() + 1) { -1L }
                                            arr[record.partition()] = record.offset()
                                            offsets += arr.toList()
                                        }
                                    }
                                    DebeziumMessage(
                                        record.offset(),
                                        Instant.ofEpochMilli(record.timestamp()),
                                        listOf(CdcEvent.fromJson(value)),
                                        offsets,
                                        // TODO: groupMetadata captured at poll-time may become stale if a rebalance
                                        //  occurs before sendOffsetsToTransaction — would cause ProducerFencedException.
                                        //  Acceptable for single-consumer setups; no data loss (transaction aborts).
                                        c.groupMetadata(),
                                    )
                                }
                            }
                        } catch (_: InterruptException) {
                            throw InterruptedException()
                        }
                    }

                    processor.processMessages(records)
                }
            }
        }

        return Log.Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
