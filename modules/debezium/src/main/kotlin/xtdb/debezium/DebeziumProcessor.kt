package xtdb.debezium

import org.apache.arrow.memory.BufferAllocator
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import xtdb.api.log.KafkaCluster
import xtdb.api.log.KafkaCluster.AtomicProducer.Companion.withTx
import xtdb.api.log.Log
import xtdb.api.log.SourceMessage
import xtdb.debezium.proto.debeziumOffsetToken
import xtdb.debezium.proto.partitionOffsets
import xtdb.tx.TxOp
import xtdb.tx.toArrowBytes
import xtdb.tx.serializeUserMetadata
import java.time.ZoneId
import xtdb.database.ExternalSourceToken
import com.google.protobuf.Any as ProtoAny

private val logger = LoggerFactory.getLogger(DebeziumProcessor::class.java)

class DebeziumProcessor(
    private val producer: KafkaCluster.AtomicProducer<SourceMessage>,
    private val allocator: BufferAllocator,
    private val defaultTz: ZoneId,
) : Log.RecordProcessor<DebeziumMessage>, AutoCloseable {

    companion object {
        fun buildOffsetToken(kafkaOffsets: Map<TopicPartition, OffsetAndMetadata>): ExternalSourceToken {
            // OffsetAndMetadata stores next-to-read (offset+1); token stores last-seen
            val byTopic = kafkaOffsets.entries.groupBy({ it.key.topic() }, { it.key.partition() to (it.value.offset() - 1) })
            val token = debeziumOffsetToken {
                for ((topic, partitionEntries) in byTopic) {
                    val maxPartition = partitionEntries.maxOf { it.first }
                    val offsetArray = LongArray(maxPartition + 1) { partition ->
                        partitionEntries.firstOrNull { it.first == partition }?.second ?: -1L
                    }
                    dbzmTopicOffsets[topic] = partitionOffsets {
                        offsets += offsetArray.toList()
                    }
                }
            }
            return ProtoAny.pack(token, "xtdb.debezium")
        }
    }

    private fun buildTxMessage(
        txOps: List<TxOp>, userMetadata: Map<String, Any>,
        externalSourceToken: ExternalSourceToken? = null
    ): SourceMessage.Tx =
        SourceMessage.Tx(
            txOps = txOps.toArrowBytes(allocator),
            systemTime = null,
            defaultTz = defaultTz,
            user = null,
            userMetadata = serializeUserMetadata(allocator, userMetadata),
            externalSourceToken = externalSourceToken
        )

    override suspend fun processRecords(records: List<Log.Record<DebeziumMessage>>) {
        for (record in records) {
            producer.withTx { tx ->
                tx.sendOffsetsToTransaction(record.message.offsets, record.message.consumerGroupMetadata)
                val event = CdcEvent.fromJson((record.message).payload)
                val token = buildOffsetToken(record.message.offsets)

                event.toTxOp(allocator).use { txOp ->
                    val metadata = mapOf(
                        "source" to "debezium",
                        "kafka_offset" to record.logOffset,
                    )
                    val message = buildTxMessage(listOf(txOp), metadata, externalSourceToken = token)
                    tx.appendMessage(message)
                    logger.debug("Submitted tx at offset {}", record.logOffset)
                }
            }
        }
    }

    override fun close() {}
}
