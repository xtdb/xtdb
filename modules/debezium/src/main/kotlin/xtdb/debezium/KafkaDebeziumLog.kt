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
import xtdb.database.ExternalSourceToken
import xtdb.debezium.proto.DebeziumOffsetToken
import xtdb.debezium.proto.KafkaDebeziumLogConfig
import xtdb.debezium.proto.debeziumOffsetToken
import xtdb.debezium.proto.kafkaDebeziumLogConfig
import xtdb.debezium.proto.partitionOffsets
import java.time.Duration
import java.time.Instant

private val LOG = LoggerFactory.getLogger(KafkaDebeziumLog::class.java)

@OptIn(ExperimentalCoroutinesApi::class)
class KafkaDebeziumLog @JvmOverloads constructor(
    private val kafkaConfig: Map<String, String>,
    private val topic: String,
    private val pollDuration: Duration = Duration.ofSeconds(1),
) : DebeziumLog {

    object UnitDeserializer : Deserializer<Unit> {
        override fun deserialize(topic: String?, data: ByteArray) = Unit
    }

    @Serializable
    @SerialName("!Kafka")
    data class Factory(
        val logCluster: LogClusterAlias, val tableTopic: String,
    ) : DebeziumLog.Factory {

        override fun openLog(clusters: Map<LogClusterAlias, Log.Cluster>): DebeziumLog {
            val cluster =
                requireNotNull(clusters[logCluster] as? KafkaCluster) { "missing Kafka cluster: '${logCluster}'" }

            AdminClient.create(cluster.kafkaConfigMap).use { admin ->
                admin.ensureTopicExists(tableTopic, autoCreate = false)
            }

            return KafkaDebeziumLog(cluster.kafkaConfigMap, tableTopic)
        }

        fun toProto(): KafkaDebeziumLogConfig = kafkaDebeziumLogConfig {
            this.logCluster = this@Factory.logCluster
            this.tableTopic = this@Factory.tableTopic
        }

        companion object {
            fun fromProto(proto: KafkaDebeziumLogConfig) =
                Factory(
                    logCluster = proto.logCluster,
                    tableTopic = proto.tableTopic,
                )
        }
    }

    val epoch: Int get() = 0

    override suspend fun tailAll(afterToken: ExternalSourceToken?, processor: MessageProcessor<DebeziumMessage>) {
        KafkaConsumer(
            kafkaConfig.plus(
                mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
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
                val tp = TopicPartition(topic, 0)
                c.assign(listOf(tp))
                c.seekToBeginning(listOf(tp))
            }

            while (currentCoroutineContext().isActive) {
                val records = runInterruptible(Dispatchers.IO) {
                    try {
                        c.poll(pollDuration)
                    } catch (_: InterruptException) {
                        throw InterruptedException()
                    }
                }

                if (!records.isEmpty()) {
                    val ops = mutableListOf<CdcEvent>()
                    var maxOffset = -1L
                    var latestTimestamp = Instant.EPOCH

                    for (consumerRecord in records) {
                        maxOffset = maxOf(maxOffset, consumerRecord.offset())
                        latestTimestamp = maxOf(latestTimestamp, Instant.ofEpochMilli(consumerRecord.timestamp()))
                        val value = consumerRecord.value() ?: continue
                        ops.add(CdcEvent.fromJson(value))
                    }

                    val offsets = debeziumOffsetToken {
                        dbzmTopicOffsets[topic] = partitionOffsets {
                            offsets += listOf(maxOffset)
                        }
                    }
                    processor.processMessages(listOf(
                        DebeziumMessage(
                            maxOffset,
                            latestTimestamp,
                            ops,
                            offsets,
                        )
                    ))
                }
            }
        }
    }

    override fun close() {
    }
}
