package xtdb.debezium

import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.RootAllocator
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
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
import xtdb.error.Incorrect
import xtdb.indexer.OpenTx
import xtdb.api.TransactionKey
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.util.asIid
import java.nio.ByteBuffer
import java.time.DateTimeException
import java.time.Duration
import java.time.Instant

private val LOG = LoggerFactory.getLogger(KafkaDebeziumLog::class.java)

private fun parseValidTimeMicros(value: Any?, field: String): Long? = when (value) {
    null -> null
    is String -> try {
        Instant.parse(value).asMicros
    } catch (e: DateTimeException) {
        throw Incorrect("Invalid ISO-8601 timestamp for '$field': $value")
    }
    else -> throw Incorrect("'$field' must be a TIMESTAMPTZ string, got ${value::class.simpleName}")
}

@OptIn(ExperimentalCoroutinesApi::class)
class KafkaDebeziumLog @JvmOverloads constructor(
    private val dbName: String,
    private val kafkaConfig: Map<String, String>,
    private val topic: String,
    private val messageFormat: MessageFormat,
    private val pollDuration: Duration = Duration.ofSeconds(1),
) : DebeziumLog {

    private val allocator = RootAllocator()

    object UnitDeserializer : Deserializer<Unit> {
        override fun deserialize(topic: String?, data: ByteArray) = Unit
    }

    @Serializable
    @SerialName("!Kafka")
    data class Factory(
        val logCluster: LogClusterAlias, val tableTopic: String,
    ) : DebeziumLog.Factory {

        override fun openLog(dbName: String, clusters: Map<LogClusterAlias, Log.Cluster>, messageFormat: MessageFormat): DebeziumLog {
            val cluster =
                requireNotNull(clusters[logCluster] as? KafkaCluster) { "missing Kafka cluster: '${logCluster}'" }

            if (messageFormat is MessageFormat.Avro) {
                requireNotNull(cluster.schemaRegistryUrl) {
                    "schemaRegistryUrl must be set on Kafka cluster '${logCluster}' when using Avro message format"
                }
            }

            AdminClient.create(cluster.kafkaConfigMap).use { admin ->
                admin.ensureTopicExists(tableTopic, autoCreate = false)
            }

            val kafkaConfig = when (messageFormat) {
                is MessageFormat.Json -> cluster.kafkaConfigMap
                is MessageFormat.Avro -> cluster.kafkaConfigMap.plus(
                    mapOf(
                        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG to cluster.schemaRegistryUrl!!,
                        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to "false",
                    )
                )
            }

            return KafkaDebeziumLog(dbName, kafkaConfig, tableTopic, messageFormat)
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

    private fun valueDeserializer(): Deserializer<*> = when (messageFormat) {
        is MessageFormat.Json -> ByteArrayDeserializer()
        // KafkaConsumer doesn't call configure() on deserializer instances passed to the constructor,
        // so we configure it manually with the Schema Registry URL from kafkaConfig.
        is MessageFormat.Avro -> KafkaAvroDeserializer().also { it.configure(kafkaConfig, false) }
    }

    @Suppress("UNCHECKED_CAST")
    private fun writeCdcPayload(payload: Map<String, Any?>, dbName: String, openTx: OpenTx) {
        val op = payload["op"] as? String
            ?: throw Incorrect("Missing 'op' in payload")

        val source = payload["source"] as? Map<String, Any?>
            ?: throw Incorrect("Missing 'source' in payload")
        val schema = source["schema"] as? String
            ?: throw Incorrect("Missing 'source.schema' in payload")
        val table = source["table"] as? String
            ?: throw Incorrect("Missing 'source.table' in payload")

        val openTxTable = openTx.table(TableRef(dbName, schema, table))

        when (op) {
            "c", "r", "u" -> {
                val after = payload["after"] as? Map<String, Any?>
                    ?: throw Incorrect("Missing 'after' for put op")

                val docMap = after.toMutableMap()

                val id = docMap["_id"] ?: throw Incorrect("Missing '_id' in document")

                val explicitValidFrom = parseValidTimeMicros(docMap.remove("_valid_from"), "_valid_from")
                val explicitValidTo = parseValidTimeMicros(docMap.remove("_valid_to"), "_valid_to")

                if (explicitValidTo != null && explicitValidFrom == null) {
                    throw Incorrect("'_valid_to' requires '_valid_from'")
                }

                openTxTable.logPut(
                    ByteBuffer.wrap(id.asIid),
                    explicitValidFrom ?: openTx.systemFrom,
                    explicitValidTo ?: Long.MAX_VALUE,
                ) { openTxTable.docWriter.writeObject(docMap) }
            }

            "d" -> {
                val before = payload["before"]?.let { it as? Map<String, Any?> }
                    ?: throw Incorrect("Missing 'before' for delete — check REPLICA IDENTITY on source table")

                val id = before["_id"]
                    ?: throw Incorrect("Missing '_id' in 'before' for delete")

                openTxTable.logDelete(
                    ByteBuffer.wrap(id.asIid),
                    openTx.systemFrom,
                    Long.MAX_VALUE,
                )
            }

            else -> throw Incorrect("Unknown CDC op: '$op'")
        }
    }

    override suspend fun tailAll(afterToken: ExternalSourceToken?, processor: MessageProcessor<DebeziumMessage>) {
        KafkaConsumer<Unit, Any>(
            kafkaConfig.plus(
                mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
                )
            ),
            UnitDeserializer,
            @Suppress("UNCHECKED_CAST") (valueDeserializer() as Deserializer<Any>)
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

                if (!records.isEmpty) {
                    // first pass: compute batch-level metadata
                    var maxOffset = -1L
                    var latestTimestamp = Instant.EPOCH
                    for (consumerRecord in records) {
                        maxOffset = maxOf(maxOffset, consumerRecord.offset())
                        latestTimestamp = maxOf(latestTimestamp, Instant.ofEpochMilli(consumerRecord.timestamp()))
                    }

                    val txKey = TransactionKey(maxOffset, latestTimestamp)

                    // second pass: parse CDC events directly into OpenTx
                    // processor takes ownership of the OpenTx (commits and closes it)
                    OpenTx(allocator, txKey).use { openTx ->
                        for (consumerRecord in records) {
                            val value = consumerRecord.value() ?: continue
                            writeCdcPayload(messageFormat.decode(value), dbName, openTx)
                        }

                        val offsets = debeziumOffsetToken {
                            dbzmTopicOffsets[topic] = partitionOffsets {
                                offsets += listOf(maxOffset)
                            }
                        }

                        processor.processMessages(listOf(
                            DebeziumMessage(maxOffset, latestTimestamp, openTx, offsets)
                        ))
                    }
                }
            }
        }
    }

    override fun close() {
        allocator.close()
    }
}
