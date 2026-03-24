package xtdb.debezium

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import xtdb.debezium.proto.DebeziumOffsetToken

class DebeziumMessage(
    val ops: List<CdcEvent>,
    val offsets: DebeziumOffsetToken,
    val consumerGroupMetadata: ConsumerGroupMetadata,
)