@file:UseSerializers(
    DurationSerde::class,
    PathSerde::class
)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runInterruptible
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import xtdb.DurationSerde
import xtdb.api.PathSerde
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.database.proto.DatabaseConfig
import xtdb.kafka.proto.KafkaLogConfig
import xtdb.kafka.proto.kafkaLogConfig
import xtdb.types.LogOffset
import xtdb.types.MessageId
import xtdb.util.MsgIdUtil.afterMsgIdToOffset
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.close
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn
import java.nio.file.Path
import java.time.Duration
import java.time.Instant.ofEpochMilli
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.inputStream
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toKotlinDuration
import com.google.protobuf.Any as ProtoAny

private val LOG = KafkaCluster::class.logger

private typealias KafkaConfigMap = Map<String, String>

private object UnitSerializer : Serializer<Unit> {
    override fun serialize(topic: String?, data: Unit) = null
}

private object UnitDeserializer : Deserializer<Unit> {
    override fun deserialize(topic: String?, data: ByteArray) = Unit
}

/**
 * The producer properties this log is opened with, given an operator's own properties.
 *
 * `acks` is applied last, so an operator property cannot weaken it. `AcceptedRecordsAreNeverWithdrawn`
 * (allium/log-processor-lifecycle.allium) makes that a safety requirement rather than a durability
 * preference: a leader's term reaches every reader only as a record on the replica log, so withdrawing an
 * acknowledged record after another node has read it lets the two disagree about which term the log has
 * reached, and the fence is nothing but that agreement. Under `acks=1` the partition leader acknowledges
 * before any follower holds the record, which is exactly such a withdrawal.
 *
 * Everything else stays overridable, `enable.idempotence` included — without it a retried append can be
 * written twice, and a duplicated term-stamped no-op is inert, because a reader takes the highest term it
 * has seen and a second copy of one it already holds moves nothing.
 */
internal fun KafkaConfigMap.producerConfig(): KafkaConfigMap =
    mapOf(
        "enable.idempotence" to "true",
        "compression.type" to "snappy",
        "linger.ms" to "0",
    ) + this + mapOf("acks" to "all")

private fun KafkaConfigMap.openProducer(): KafkaProducer<Unit, ByteArray> {
    // -1 is Kafka's synonym for all, and asks for the same thing.
    this["acks"]?.takeUnless { it == "all" || it == "-1" }?.let {
        LOG.warn(
            "Kafka 'acks' is set to '$it' in this cluster's properties and is being disregarded: XTDB's " +
                    "logs require 'all', because an acknowledgement a truncation can withdraw lets two " +
                    "nodes reach different conclusions about which of them leads a database."
        )
    }

    return KafkaProducer(producerConfig(), UnitSerializer, ByteArraySerializer())
}

private fun KafkaConsumer<*, *>.seekToAfterMsgId(tp: TopicPartition, epoch: Int, afterMsgId: MessageId) {
    val previousOffset = afterMsgIdToOffset(epoch, afterMsgId)
    if (previousOffset < 0L) seekToBeginning(listOf(tp)) else seek(tp, previousOffset + 1)
}

private fun KafkaConfigMap.openConsumer() =
    KafkaConsumer(
        mapOf(
            "enable.auto.commit" to "false",
            "isolation.level" to "read_committed",
            "auto.offset.reset" to "none",
        ) + this,
        UnitDeserializer,
        ByteArrayDeserializer()
    )

private fun AdminClient.ensureTopicExists(topic: String, autoCreate: Boolean) {
    val desc =
        try {
            describeTopics(listOf(topic)).allTopicNames().get()[topic]
        } catch (e: ExecutionException) {
            try {
                throw e.cause ?: e
            } catch (_: UnknownTopicOrPartitionException) {
                null
            } catch (e: Throwable) {
                throw e
            }
        }

    when {
        desc != null -> {
            check(desc.partitions().size == 1) { "Topic $topic must have exactly one partition" }
            warnIfTruncatable(topic, desc.partitions().single().replicas().size)
        }

        autoCreate -> {
            val newTopic = NewTopic(topic, 1, 1)
                .configs(mapOf("message.timestamp.type" to "LogAppendTime"))

            createTopics(listOf(newTopic))
        }

        else -> error("Topic $topic does not exist, auto-create set to false")
    }
}

/**
 * Reports the two topic settings which leave an acknowledged record truncatable in spite of `acks=all`.
 *
 * Warned rather than corrected, unlike [producerConfig]: this is the operator's cluster rather than
 * XTDB's own configuration, so saying loudly what is wrong is the whole of what we can do about it — and
 * what `DurabilityComesFromDeployedConfiguration` (allium/log-processor-lifecycle.allium) asks for wherever the log
 * can report its own settings.
 *
 * Single-replica topics are skipped entirely. Neither setting has anything to act on without a second
 * replica to fail over to, and one replica is what [ensureTopicExists] itself creates, so checking would
 * mean warning about our own default on every dev node.
 */
private fun AdminClient.warnIfTruncatable(topic: String, replicas: Int) {
    if (replicas < 2) return

    val resource = ConfigResource(ConfigResource.Type.TOPIC, topic)

    // Reading a topic's configuration needs DescribeConfigs, which a narrowly-scoped principal may well
    // not hold. This check is advisory, so being refused it MUST NOT stop the node connecting.
    val config =
        try {
            describeConfigs(listOf(resource)).all().get()[resource]
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            LOG.warn(e, "could not read $topic's configuration to check XTDB's durability requirements")
            null
        } ?: return

    val minIsr = config.get("min.insync.replicas")?.value()?.toIntOrNull()
    if (minIsr != null && minIsr < 2)
        LOG.warn(
            "Topic $topic has $replicas replicas but min.insync.replicas=$minIsr, so a write is " +
                    "acknowledged once the partition leader alone holds it, and is lost if that broker " +
                    "fails. Set min.insync.replicas to at least 2."
        )

    if (config.get("unclean.leader.election.enable")?.value() == "true")
        LOG.warn(
            "Topic $topic permits unclean leader election, which can truncate records XTDB has already " +
                    "been told are durable. Set unclean.leader.election.enable=false."
        )
}

class KafkaCluster(
    val kafkaConfigMap: KafkaConfigMap,
    private val pollDuration: Duration,
    val schemaRegistryUrl: String? = null,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Remote {
    val producer = kafkaConfigMap.openProducer()
    val scope = CoroutineScope(SupervisorJob() + coroutineContext)

    override fun close() {
        try {
            runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        } finally {
            producer.close()
        }
    }

    @Serializable
    @SerialName("!Kafka")
    data class ClusterFactory @JvmOverloads constructor(
        val bootstrapServers: String,
        var pollDuration: Duration = Duration.ofSeconds(1),
        var propertiesMap: Map<String, String> = emptyMap(),
        var propertiesFile: Path? = null,
        var schemaRegistryUrl: String? = null,
        @kotlinx.serialization.Transient var coroutineContext: CoroutineContext = Dispatchers.Default
    ) : Remote.Factory<KafkaCluster> {

        fun pollDuration(pollDuration: Duration) = apply { this.pollDuration = pollDuration }
        fun propertiesMap(propertiesMap: Map<String, String>) = apply { this.propertiesMap = propertiesMap }
        fun propertiesFile(propertiesFile: Path) = apply { this.propertiesFile = propertiesFile }
        fun schemaRegistryUrl(schemaRegistryUrl: String) = apply { this.schemaRegistryUrl = schemaRegistryUrl }

        private val Path.asPropertiesMap: Map<String, String>
            get() =
                Properties()
                    .apply { load(inputStream()) }
                    .entries.associate { it.key as String to it.value as String }

        private val configMap: Map<String, String>
            get() = mapOf("bootstrap.servers" to bootstrapServers)
                .plus(propertiesMap)
                .plus(propertiesFile?.asPropertiesMap.orEmpty())

        override fun open(): KafkaCluster =
            KafkaCluster(configMap, pollDuration, schemaRegistryUrl, coroutineContext)
    }

    private inner class KafkaLog<M>(
        private val codec: MessageCodec<M>,
        private val topic: String,
        override val epoch: Int,
    ) : Log<M> {

        // The operator's own knob, and now also how fast the election clock ticks: it is what a reader's
        // empty read waits for, and a participant measures quiet across its own reads. Raised past the
        // election-timeout spread, every follower tips over on the same read and the randomisation that
        // separates them buys nothing.
        override val tailPollDuration = pollDuration.toKotlinDuration()

        private fun readLatestSubmittedMessage(kafkaConfigMap: KafkaConfigMap): LogOffset =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                (c.endOffsets(listOf(tp))[tp] ?: 0) - 1
            }

        private val latestSubmittedOffset0 = AtomicLong(readLatestSubmittedMessage(kafkaConfigMap))
        override fun latestSubmittedOffset(partition: Int) = latestSubmittedOffset0.get()

        override suspend fun appendMessage(message: M, partition: Int): Log.MessageMetadata =
            CompletableDeferred<Log.MessageMetadata>()
                .also { res ->
                    producer.send(
                        ProducerRecord(topic, null, Unit, codec.encode(message))
                    ) { recordMetadata, e ->
                        if (e == null) {
                            val metadata = Log.MessageMetadata(
                                epoch,
                                recordMetadata.offset(),
                                ofEpochMilli(recordMetadata.timestamp())
                            )
                            latestSubmittedOffset0.updateAndGet { it.coerceAtLeast(metadata.logOffset) }
                            res.complete(metadata)
                        } else res.completeExceptionally(e)
                    }
                }
                .await()

        override fun readLastMessage(partition: Int): M? =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                val lastOffset = c.endOffsets(listOf(tp))[tp]?.minus(1)?.takeIf { it >= 0 } ?: return null
                c.assign(listOf(tp))
                c.seek(tp, lastOffset)

                val records = c.poll(pollDuration).records(topic)
                records.firstOrNull()?.let { record -> codec.decode(record.value()) }
            }

        override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId) = sequence {
            if (msgIdToEpoch(fromMsgId) != epoch || msgIdToEpoch(toMsgId) != epoch) return@sequence
            val fromOffset = msgIdToOffset(fromMsgId)
            val toOffset = msgIdToOffset(toMsgId)
            if (fromOffset >= toOffset) return@sequence

            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                c.assign(listOf(tp))

                val endOffset = c.endOffsets(listOf(tp))[tp] ?: 0
                val effectiveToOffset = minOf(toOffset, endOffset)
                if (fromOffset >= effectiveToOffset) return@sequence

                c.seek(tp, fromOffset)

                while (c.position(tp) < effectiveToOffset) {
                    for (rec in c.poll(pollDuration).records(topic)) {
                        if (rec.offset() >= effectiveToOffset) return@sequence
                        val msg = codec.decode(rec.value()) ?: continue
                        yield(Log.Record(epoch, rec.offset(), ofEpochMilli(rec.timestamp()), msg))
                    }
                }
            }
        }

        private fun KafkaConsumer<*, ByteArray>.pollRecords(): List<Log.Record<M>> =
            try {
                poll(pollDuration)
                    .records(topic)
                    .mapNotNull { rec ->
                        Log.Record(
                            epoch, rec.offset(), ofEpochMilli(rec.timestamp()),
                            codec.decode(rec.value()) ?: return@mapNotNull null
                        )
                    }
            } catch (_: WakeupException) {
                emptyList()
            } catch (e: InterruptException) {
                throw InterruptedException().initCause(e)
            }

        override suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: Log.RecordProcessor<M>) = coroutineScope {
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                c.assign(listOf(tp))
                c.seekToAfterMsgId(tp, epoch, afterMsgId)

                while (isActive) {
                    // The empty batch is delivered deliberately: a participant measures quiet across its
                    // own reads, so a log that spoke only when it had something to say would leave a node
                    // no longer being delivered to indistinguishable from an idle one.
                    val records = runInterruptible(Dispatchers.IO) { c.pollRecords() }
                    processor.processRecords(records)
                }
            }
        }

        override fun close() = Unit
    }

    @Serializable
    @SerialName("!Kafka")
    data class LogFactory @JvmOverloads constructor(
        val cluster: RemoteAlias,
        val topic: String,
        var replicaCluster: RemoteAlias = cluster,
        var replicaTopic: String = "$topic-replica",
        var autoCreateTopic: Boolean = true,
        var epoch: Int = 0,
    ) : Log.Factory {

        fun replicaCluster(replicaCluster: RemoteAlias) = apply { this.replicaCluster = replicaCluster }
        fun replicaTopic(replicaTopic: String) = apply { this.replicaTopic = replicaTopic }
        fun autoCreateTopic(autoCreateTopic: Boolean) = apply { this.autoCreateTopic = autoCreateTopic }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int): Log<SourceMessage> {
            val clusterAlias = this.cluster
            val cluster = requireNotNull(remotes[clusterAlias] as? KafkaCluster) {
                "missing Kafka cluster: '$clusterAlias'"
            }

            val configMap = cluster.kafkaConfigMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(topic, autoCreateTopic)
            }

            return cluster.KafkaLog(SourceMessage.Codec, topic, epoch)
        }

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openSourceLog(remotes, partitions))

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int): Log<ReplicaMessage> {
            val clusterAlias = this.replicaCluster
            val cluster = requireNotNull(remotes[clusterAlias] as? KafkaCluster) {
                "missing Kafka cluster: '$clusterAlias'"
            }

            val configMap = cluster.kafkaConfigMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(replicaTopic, autoCreateTopic)
            }

            return cluster.KafkaLog(ReplicaMessage.Codec, replicaTopic, epoch)
        }

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openReplicaLog(remotes, partitions))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.setOtherLog(ProtoAny.pack(kafkaLogConfig {
                this.topic = this@LogFactory.topic
                this.epoch = this@LogFactory.epoch
                this.logClusterAlias = cluster
                this.replicaClusterAlias = replicaCluster
                this.replicaTopic = this@LogFactory.replicaTopic
            }, "proto.xtdb.com"))
        }
    }

    /**
     * @suppress
     */
    class Registration : Log.Registration {
        override val protoTag: String get() = "proto.xtdb.com/xtdb.kafka.proto.KafkaLogConfig"

        override fun fromProto(msg: ProtoAny) =
            msg.unpack(KafkaLogConfig::class.java).let {
                LogFactory(it.logClusterAlias, it.topic).apply {
                    epoch = it.epoch
                    if (it.hasReplicaClusterAlias()) replicaCluster = it.replicaClusterAlias
                    if (it.hasReplicaTopic()) replicaTopic = it.replicaTopic
                }
            }

        override fun registerSerde(builder: PolymorphicModuleBuilder<Log.Factory>) {
            builder.subclass(LogFactory::class)
        }
    }

    /**
     * @suppress
     */
    class ClusterRegistration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(ClusterFactory::class)
        }
    }
}
