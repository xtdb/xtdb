package xtdb.debezium

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG
import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import xtdb.indexer.TxIndexer
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.KafkaCluster
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.ensureTopicExists
import xtdb.database.ExternalSource
import xtdb.indexer.TxIndexer.TxResult
import xtdb.database.ExternalSourceToken
import xtdb.database.proto.DatabaseConfig
import xtdb.debezium.proto.*
import xtdb.debezium.proto.DebeziumKafkaSourceConfig.MessageFormatCase
import xtdb.error.Incorrect
import xtdb.indexer.TxIndexer.OpenTx
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.util.asIid
import java.nio.ByteBuffer
import java.time.DateTimeException
import java.time.Duration
import java.time.Instant
import com.google.protobuf.Any as ProtoAny

private val LOG = LoggerFactory.getLogger(DebeziumKafkaSource::class.java)

private fun parseValidTimeMicros(value: Any?, field: String): Long? = when (value) {
    null -> null
    is String -> try {
        Instant.parse(value).asMicros
    } catch (_: DateTimeException) {
        throw Incorrect("Invalid ISO-8601 timestamp for '$field': $value")
    }

    else -> throw Incorrect("'$field' must be a TIMESTAMPTZ string, got ${value::class.simpleName}")
}

@OptIn(ExperimentalCoroutinesApi::class)
class DebeziumKafkaSource @JvmOverloads constructor(
    private val dbName: String,
    private val kafkaConfig: Map<String, String>,
    private val topic: String,
    private val messageFormat: MessageFormat,
    private val pollDuration: Duration = Duration.ofSeconds(1),
) : ExternalSource {

    object UnitDeserializer : Deserializer<Unit> {
        override fun deserialize(topic: String?, data: ByteArray) = Unit
    }

    private val valueDeserializer: Deserializer<out Any> = when (messageFormat) {
        MessageFormat.Json -> ByteArrayDeserializer()
        MessageFormat.Avro -> KafkaAvroDeserializer().also { it.configure(kafkaConfig, false) }
    }

    @Serializable
    @SerialName("!DebeziumKafka")
    data class Factory(
        val logCluster: LogClusterAlias,
        val tableTopic: String,
        val messageFormat: MessageFormat,
    ) : ExternalSource.Factory {

        override fun open(
            dbName: String,
            clusters: Map<LogClusterAlias, Log.Cluster>,
            remotes: Map<RemoteAlias, Remote>,
        ): ExternalSource {
            val cluster =
                requireNotNull(clusters[logCluster] as? KafkaCluster) { "missing Kafka cluster: '${logCluster}'" }

            AdminClient.create(cluster.kafkaConfigMap).use { it.ensureTopicExists(tableTopic, autoCreate = false) }

            val kafkaConfig = when (messageFormat) {
                is MessageFormat.Json -> cluster.kafkaConfigMap

                is MessageFormat.Avro -> {
                    val schemaRegUrl = requireNotNull(cluster.schemaRegistryUrl) {
                        "schemaRegistryUrl must be set on Kafka cluster '${logCluster}' when using Avro message format"
                    }

                    cluster.kafkaConfigMap
                        .plus(mapOf(SCHEMA_REGISTRY_URL_CONFIG to schemaRegUrl, SPECIFIC_AVRO_READER_CONFIG to "false"))
                }
            }

            return DebeziumKafkaSource(dbName, kafkaConfig, tableTopic, messageFormat)
        }

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.externalSource = ProtoAny.pack(debeziumKafkaSourceConfig {
                logCluster = this@Factory.logCluster
                tableTopic = this@Factory.tableTopic
                when (messageFormat) {
                    MessageFormat.Json -> json = true
                    MessageFormat.Avro -> avro = true
                }
            }, "proto.xtdb.com")
        }

        class Registration : ExternalSource.Registration {
            override val protoTag: String get() = "proto.xtdb.com/xtdb.debezium.proto.DebeziumKafkaSourceConfig"

            override fun fromProto(msg: ProtoAny): ExternalSource.Factory {
                val config = msg.unpack(DebeziumKafkaSourceConfig::class.java)

                return Factory(
                    logCluster = config.logCluster,
                    tableTopic = config.tableTopic,

                    messageFormat = when (config.messageFormatCase) {
                        MessageFormatCase.AVRO -> MessageFormat.Avro
                        MessageFormatCase.JSON -> MessageFormat.Json

                        else -> error("unknown message format in DebeziumKafkaSourceConfig: ${config.messageFormatCase}")
                    },
                )
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class)
            }
        }
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

                if (explicitValidTo != null && explicitValidFrom == null)
                    throw Incorrect("'_valid_to' requires '_valid_from'")

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

    override suspend fun onPartitionAssigned(
        partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
    ) {
        KafkaConsumer(
            kafkaConfig.plus(
                mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
                )
            ),
            UnitDeserializer, valueDeserializer
        ).use { c ->
            // TODO: use partition parameter for multi-partition support
            val token = afterToken?.unpack(DebeziumOffsetToken::class.java)

            val partitionOffsets = token?.dbzmTopicOffsetsMap?.get(topic)?.offsetsList
                ?.mapIndexedNotNull { p, offset ->
                    if (offset >= 0) TopicPartition(topic, p) to offset else null
                }
                ?.takeIf { it.isNotEmpty() }

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
                    var maxOffset = -1L
                    var latestTimestamp = Instant.EPOCH
                    for (consumerRecord in records) {
                        maxOffset = maxOf(maxOffset, consumerRecord.offset())
                        latestTimestamp = maxOf(latestTimestamp, Instant.ofEpochMilli(consumerRecord.timestamp()))
                    }

                    val resumeToken: ExternalSourceToken = ProtoAny.pack(debeziumOffsetToken {
                        dbzmTopicOffsets[topic] = partitionOffsets {
                            offsets += listOf(maxOffset)
                        }
                    }, "xtdb.debezium")

                    txIndexer.indexTx(resumeToken, latestTimestamp) { openTx ->
                        for (consumerRecord in records) {
                            val value = consumerRecord.value() ?: continue
                            writeCdcPayload(messageFormat.decode(value), dbName, openTx)
                        }
                        TxResult.Committed(
                            userMetadata = mapOf("lsn" to maxOffset, "tx_us" to latestTimestamp.asMicros)
                        )
                    }
                }
            }
        }
    }

    override fun close() = Unit
}
