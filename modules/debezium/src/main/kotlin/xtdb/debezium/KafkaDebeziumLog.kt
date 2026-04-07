package xtdb.debezium

import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import org.apache.arrow.memory.RootAllocator
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

private fun JsonElement.toJvmValue(): Any? = when (this) {
    is JsonNull -> null
    is JsonPrimitive -> if (isString) content else booleanOrNull ?: longOrNull ?: doubleOrNull ?: content
    is JsonArray -> map { it.toJvmValue() }
    is JsonObject -> entries.associate { (k, v) -> k to v.toJvmValue() }
}

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

        override fun openLog(dbName: String, clusters: Map<LogClusterAlias, Log.Cluster>): DebeziumLog {
            val cluster =
                requireNotNull(clusters[logCluster] as? KafkaCluster) { "missing Kafka cluster: '${logCluster}'" }

            AdminClient.create(cluster.kafkaConfigMap).use { admin ->
                admin.ensureTopicExists(tableTopic, autoCreate = false)
            }

            return KafkaDebeziumLog(dbName, cluster.kafkaConfigMap, tableTopic)
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

    private fun writeCdcJson(bytes: ByteArray, dbName: String, openTx: OpenTx) {
        val envelope = try {
            Json.parseToJsonElement(String(bytes)).jsonObject
        } catch (e: IllegalArgumentException) {
            throw Incorrect("Invalid CDC message: ${e.message}", cause = e)
        }

        val payload = envelope["payload"]?.jsonObject ?: envelope

        val op = payload["op"]?.jsonPrimitive?.content
            ?: throw Incorrect("Missing 'op' in payload")

        val source = payload["source"]?.jsonObject
            ?: throw Incorrect("Missing 'source' in payload")
        val schema = source["schema"]?.jsonPrimitive?.content
            ?: throw Incorrect("Missing 'source.schema' in payload")
        val table = source["table"]?.jsonPrimitive?.content
            ?: throw Incorrect("Missing 'source.table' in payload")

        val openTxTable = openTx.table(TableRef(dbName, schema, table))

        when (op) {
            "c", "r", "u" -> {
                val after = payload["after"]?.jsonObject
                    ?: throw Incorrect("Missing 'after' for put op")

                val docMap = after.entries.associate { (k, v) -> k to v.toJvmValue() }.toMutableMap()

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
                val before = payload["before"]?.takeUnless { it is JsonNull }?.jsonObject
                    ?: throw Incorrect("Missing 'before' for delete — check REPLICA IDENTITY on source table")

                val id = before["_id"]?.toJvmValue()
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
                            writeCdcJson(value, dbName, openTx)
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
