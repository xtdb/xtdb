package xtdb.debezium

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import xtdb.api.TxId
import xtdb.debezium.proto.DebeziumOffsetToken
import java.time.Instant

class DebeziumMessage(
    val txId: TxId,
    val systemTime: Instant,
    val ops: List<CdcEvent>,
    val offsets: DebeziumOffsetToken,
    val consumerGroupMetadata: ConsumerGroupMetadata,
)