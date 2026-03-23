package xtdb.debezium

import org.apache.arrow.memory.BufferAllocator
import org.slf4j.LoggerFactory
import xtdb.api.log.KafkaCluster
import xtdb.api.log.KafkaCluster.AtomicProducer.Companion.withTx
import xtdb.api.log.Log
import xtdb.api.log.SourceMessage
import xtdb.tx.TxOp
import xtdb.tx.toArrowBytes
import xtdb.tx.serializeUserMetadata
import java.time.ZoneId

private val logger = LoggerFactory.getLogger(DebeziumProcessor::class.java)

class DebeziumProcessor(
    private val producer: KafkaCluster.AtomicProducer<SourceMessage>,
    private val allocator: BufferAllocator,
    private val defaultTz: ZoneId,
) : Log.RecordProcessor<DebeziumMessage>, AutoCloseable {

    private fun buildTxMessage(txOps: List<TxOp>, userMetadata: Map<String, Any>): SourceMessage.Tx =
        SourceMessage.Tx(
            txOps = txOps.toArrowBytes(allocator),
            systemTime = null,
            defaultTz = defaultTz,
            user = null,
            userMetadata = serializeUserMetadata(allocator, userMetadata)
        )

    override suspend fun processRecords(records: List<Log.Record<DebeziumMessage>>) {
        for (record in records) {
            producer.withTx { tx ->
                tx.sendOffsetsToTransaction(record.message.offsets, record.message.consumerGroupMetadata)
                val event = CdcEvent.fromJson((record.message).payload)

                event.toTxOp(allocator).use { txOp ->
                    val metadata = mapOf(
                        "source" to "debezium",
                        "kafka_offset" to record.logOffset,
                    )
                    val message = buildTxMessage(listOf(txOp), metadata)
                    tx.appendMessage(message)
                    logger.debug("Submitted tx at offset {}", record.logOffset)
                }
            }
        }
    }

    override fun close() {}
}
