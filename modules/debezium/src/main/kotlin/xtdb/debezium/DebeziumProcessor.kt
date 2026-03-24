package xtdb.debezium

import org.apache.arrow.memory.BufferAllocator
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import xtdb.api.log.KafkaCluster
import xtdb.api.log.KafkaCluster.AtomicProducer.Companion.withTx
import xtdb.api.log.Log
import xtdb.api.log.SourceMessage
import xtdb.tx.TxOp
import xtdb.tx.toArrowBytes
import xtdb.tx.serializeUserMetadata
import xtdb.util.safeMap
import xtdb.util.useAll
import java.time.ZoneId
import xtdb.database.ExternalSourceToken
import com.google.protobuf.Any as ProtoAny

private val logger = LoggerFactory.getLogger(DebeziumProcessor::class.java)

class DebeziumProcessor(
    private val producer: KafkaCluster.AtomicProducer<SourceMessage>,
    private val allocator: BufferAllocator,
    private val defaultTz: ZoneId,
) : Log.RecordProcessor<DebeziumMessage>, AutoCloseable {

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
                val msg = record.message
                val kafkaOffsets = msg.offsets.dbzmTopicOffsetsMap.flatMap { (topic, partOffsets) ->
                    partOffsets.offsetsList.mapIndexedNotNull { partition, offset ->
                        if (offset >= 0) TopicPartition(topic, partition) to OffsetAndMetadata(offset + 1) else null
                    }
                }.toMap()
                tx.sendOffsetsToTransaction(kafkaOffsets, msg.consumerGroupMetadata)
                val token = ProtoAny.pack(msg.offsets, "xtdb.debezium")

                record.message.ops.safeMap { it.toTxOp(allocator) }.useAll { txOps ->
                    val metadata = mapOf(
                        "source" to "debezium",
                        "kafka_offset" to record.logOffset,
                    )
                    val message = buildTxMessage(txOps, metadata, externalSourceToken = token)
                    tx.appendMessage(message)
                    logger.debug("Submitted tx with {} ops at offset {}", txOps.size, record.logOffset)
                }
            }
        }
    }

    override fun close() {}
}
