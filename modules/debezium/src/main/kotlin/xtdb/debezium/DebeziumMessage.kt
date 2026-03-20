package xtdb.debezium

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

class DebeziumMessage(
    val payload: ByteArray,
    val offsets: Map<TopicPartition, OffsetAndMetadata>,
    val consumerGroupMetadata: ConsumerGroupMetadata,
)