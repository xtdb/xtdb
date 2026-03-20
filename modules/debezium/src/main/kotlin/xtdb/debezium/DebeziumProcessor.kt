package xtdb.debezium

import org.apache.arrow.memory.BufferAllocator
import org.slf4j.LoggerFactory
import xtdb.api.log.KafkaCluster
import xtdb.api.log.KafkaCluster.AtomicProducer.Companion.withTx
import xtdb.api.log.Log
import xtdb.api.log.SourceMessage
import xtdb.error.Incorrect
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import xtdb.tx.toBytes
import java.time.ZoneId

private val logger = LoggerFactory.getLogger(DebeziumProcessor::class.java)

class DebeziumProcessor(
    private val producer: KafkaCluster.AtomicProducer<SourceMessage>,
    private val allocator: BufferAllocator,
    private val defaultTz: ZoneId,
) : Log.RecordProcessor<DebeziumMessage>, AutoCloseable {

    override suspend fun processRecords(records: List<Log.Record<DebeziumMessage>>) {
        for (record in records) {
            producer.withTx { tx ->
                try {
                    tx.sendOffsetsToTransaction(record.message.offsets, record.message.consumerGroupMetadata)
                    val event = CdcEvent.fromJson((record.message).payload)

                    event.toTxOp(allocator).use { txOp ->
                        val txOpts = TxOpts(
                            defaultTz = defaultTz,
                            userMetadata = mapOf(
                                "source" to "debezium",
                                "kafka_offset" to record.logOffset,
                            )
                        )
                        val txOps = listOf(txOp)
                        val message = SourceMessage.Tx(txOps.toBytes(allocator, txOpts))
                        tx.appendMessage(message)
                        logger.debug("Submitted tx at offset {}", record.logOffset)
                    }
                } catch (e: Incorrect) {
                    logger.error("Failed to process CDC record at offset {}: {}", record.logOffset, e.message, e)
                    val txOpts = TxOpts(
                        defaultTz = defaultTz,
                        userMetadata = mapOf(
                            "source" to "debezium",
                            "error" to (e.message ?: "Unknown error"),
                            "kafka_offset" to record.logOffset,
                        )
                    )
                    // TODO: Currently: DLQ txs have committed=true and error in the user metadata.
                    //       In the future we will mark these transactions as errors in the usual way
                    val message = SourceMessage.Tx(emptyList<TxOp>().toBytes(allocator, txOpts))
                    tx.appendMessage(message)
                }
            }
        }
    }

    override fun close() {}
}
