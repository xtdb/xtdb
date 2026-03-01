package xtdb.debezium

import kotlinx.serialization.json.*
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

    override fun processRecords(records: List<Log.Record<SourceMessage>>) {
        for (record in records) {
            try {
                val json = String((record.message as SourceMessage.Tx).payload)

                val event = try {
                    val envelope = Json.parseToJsonElement(json).jsonObject
                    CdcEvent.fromJson(envelope)
                } catch (e: IllegalArgumentException) {
                    // Covers JsonDecodingException (malformed JSON) and .jsonObject on non-objects
                    throw Incorrect("Invalid CDC message: ${e.message}", cause = e)
                }

                event.toTxOp(allocator).use { txOp ->
                    val metadata = event.metadata + ("kafka_offset" to record.logOffset)
                    val result = node.submitTx(dbName, listOf(txOp), TxOpts(userMetadata = metadata))
                    logger.debug("Submitted tx {} at offset {}", result.txId, record.logOffset)
                }
            } catch (e: Incorrect) {
                logger.error("Failed to process CDC record at offset {}: {}", record.logOffset, e.message, e)
                submitDlq(e.message ?: "Unknown error", record.logOffset)
            }
        }
    }

    // TODO: Currently: DLQ txs have committed=true and error in the user metadata.
    //       In the future we will fail somewhere in the indexer so that the error shows there.
    private fun submitDlq(errorMessage: String, logOffset: Long) {
        node.submitTx(
            dbName, emptyList(),
            TxOpts(
                userMetadata = mapOf(
                    "source" to "debezium",
                    "error" to errorMessage,
                    "kafka_offset" to logOffset,
                )
            )
        )
    }

    override fun close() {}
}
