package xtdb.debezium

import org.apache.arrow.memory.BufferAllocator
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import xtdb.api.TransactionKey
import xtdb.api.log.KafkaCluster
import xtdb.api.log.KafkaCluster.AtomicProducer.Companion.withTx
import xtdb.api.log.SourceMessage
import xtdb.database.DatabaseName
import xtdb.database.ExternalSourceToken
import xtdb.indexer.LiveIndex
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.tx.TxOp
import xtdb.tx.serializeUserMetadata
import xtdb.tx.toArrowBytes
import xtdb.util.asIid
import xtdb.util.safeMap
import xtdb.util.useAll
import java.nio.ByteBuffer
import java.time.ZoneId
import com.google.protobuf.Any as ProtoAny

private val logger = LoggerFactory.getLogger(DebeziumProcessor::class.java)

fun LiveIndex.Tx.indexCdcEvent(
    dbName: DatabaseName, event: CdcEvent, txKey: TransactionKey
) {
    val table = TableRef(dbName, event.schema, event.table)
    val liveTableTx = liveTable(table)

    when (event) {
        is CdcEvent.Put -> {
            val iid = ByteBuffer.wrap(event.doc["_id"]!!.asIid)
            val validFrom = event.validFrom?.asMicros ?: txKey.systemTime.asMicros
            val validTo = event.validTo?.asMicros ?: Long.MAX_VALUE
            liveTableTx.logPut(iid, validFrom, validTo) { liveTableTx.docWriter.writeObject(event.doc) }
        }

        is CdcEvent.Delete -> {
            val iid = ByteBuffer.wrap(event.id.asIid)
            liveTableTx.logDelete(iid, txKey.systemTime.asMicros, Long.MAX_VALUE)
        }
    }
}

class DebeziumProcessor(
    private val producer: KafkaCluster.AtomicProducer<SourceMessage>,
    private val allocator: BufferAllocator,
    private val defaultTz: ZoneId,
) : AutoCloseable {

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

    suspend fun processMessages(messages: List<DebeziumMessage>) {
        for (msg in messages) {
            producer.withTx { tx ->
                val kafkaOffsets = msg.offsets.dbzmTopicOffsetsMap.flatMap { (topic, partOffsets) ->
                    partOffsets.offsetsList.mapIndexedNotNull { partition, offset ->
                        if (offset >= 0) TopicPartition(topic, partition) to OffsetAndMetadata(offset + 1) else null
                    }
                }.toMap()
                tx.sendOffsetsToTransaction(kafkaOffsets, msg.consumerGroupMetadata)
                val token = ProtoAny.pack(msg.offsets, "xtdb.debezium")

                msg.ops.safeMap { it.toTxOp(allocator) }.useAll { txOps ->
                    val metadata = mapOf(
                        "source" to "debezium",
                        "kafka_offset" to msg.txId,
                    )
                    val message = buildTxMessage(txOps, metadata, externalSourceToken = token)
                    tx.appendMessage(message)
                    logger.debug("Submitted tx with {} ops at txId {}", txOps.size, msg.txId)
                }
            }
        }
    }

    override fun close() {}
}
