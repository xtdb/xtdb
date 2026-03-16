package xtdb.debezium

import org.apache.arrow.memory.BufferAllocator
import org.slf4j.LoggerFactory
import xtdb.api.Xtdb
import xtdb.api.log.Log
import xtdb.api.log.SourceMessage
import xtdb.error.Incorrect
import xtdb.tx.TxOpts

private val logger = LoggerFactory.getLogger(DebeziumProcessor::class.java)

class DebeziumProcessor(
    private val node: Xtdb,
    private val dbName: String,
    private val allocator: BufferAllocator,
) : Log.RecordProcessor<SourceMessage>, AutoCloseable {

    override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
        for (record in records) {
            try {
                val event = CdcEvent.fromJson((record.message as SourceMessage.Tx).payload)

                event.toTxOp(allocator).use { txOp ->
                    val metadata = event.metadata + ("kafka_offset" to record.logOffset)
                    val result = node.submitTx(dbName, listOf(txOp), TxOpts(userMetadata = metadata))
                    logger.debug("Submitted tx {} at offset {}", result.txId, record.logOffset)
                }
            } catch (e: Incorrect) {
                logger.error("Failed to process CDC record at offset {}: {}", record.logOffset, e.message, e)
                val metadata = mapOf(
                    "source" to "debezium",
                    "error" to (e.message ?: "Unknown error"),
                    "kafka_offset" to record.logOffset,
                )
                // TODO: Currently: DLQ txs have committed=true and error in the user metadata.
                //       In the future we will mark these transactions as errors in the usual way
                node.submitTx(dbName, emptyList(), TxOpts(userMetadata = metadata))
            }
        }
    }

    override fun close() {}
}
