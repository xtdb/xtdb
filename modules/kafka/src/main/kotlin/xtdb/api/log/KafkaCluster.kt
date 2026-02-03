@file:UseSerializers(
    StringMapWithEnvVarsSerde::class,
    DurationSerde::class,
    StringWithEnvVarSerde::class,
    PathWithEnvVarSerde::class
)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringMapWithEnvVarsSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.log.Log.*
import xtdb.database.proto.DatabaseConfig
import xtdb.kafka.proto.KafkaLogConfig
import xtdb.kafka.proto.kafkaLogConfig
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.inputStream
import kotlin.time.Duration.Companion.seconds
import com.google.protobuf.Any as ProtoAny

private typealias KafkaConfigMap = Map<String, String>

private object UnitSerializer : Serializer<Unit> {
    override fun serialize(topic: String?, data: Unit) = null
}

private object UnitDeserializer : Deserializer<Unit> {
    override fun deserialize(topic: String?, data: ByteArray) = Unit
}

private fun KafkaConfigMap.openProducer() =
    KafkaProducer(
        mapOf(
            "enable.idempotence" to "true",
            "acks" to "all",
            "compression.type" to "snappy",
        ) + this,
        UnitSerializer,
        ByteArraySerializer()
    )

private fun KafkaConfigMap.openConsumer() =
    KafkaConsumer(
        mapOf(
            "enable.auto.commit" to "false",
            "isolation.level" to "read_committed",
            "auto.offset.reset" to "latest",
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
        }

        autoCreate -> {
            val newTopic = NewTopic(topic, 1, 1)
                .configs(mapOf("message.timestamp.type" to "LogAppendTime"))

            createTopics(listOf(newTopic))
        }

        else -> error("Topic $topic does not exist, auto-create set to false")
    }
}

class KafkaCluster(
    val kafkaConfigMap: KafkaConfigMap,
    private val pollDuration: Duration,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Cluster {
    val producer = kafkaConfigMap.openProducer()
    val scope = CoroutineScope(SupervisorJob() + coroutineContext)

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        producer.close()
    }

    @Serializable
    @SerialName("!Kafka")
    data class ClusterFactory @JvmOverloads constructor(
        val bootstrapServers: String,
        var pollDuration: Duration = Duration.ofSeconds(1),
        var propertiesMap: Map<String, String> = emptyMap(),
        var propertiesFile: Path? = null,
        @kotlinx.serialization.Transient var coroutineContext: CoroutineContext = Dispatchers.Default
    ) : Cluster.Factory<KafkaCluster> {

        fun pollDuration(pollDuration: Duration) = apply { this.pollDuration = pollDuration }
        fun propertiesMap(propertiesMap: Map<String, String>) = apply { this.propertiesMap = propertiesMap }
        fun propertiesFile(propertiesFile: Path) = apply { this.propertiesFile = propertiesFile }
        fun coroutineContext(coroutineContext: CoroutineContext) = apply { this.coroutineContext = coroutineContext }

        private val Path.asPropertiesMap: Map<String, String>
            get() =
                Properties()
                    .apply { load(inputStream()) }
                    .entries.associate { it.key as String to it.value as String }

        private val configMap: Map<String, String>
            get() = mapOf("bootstrap.servers" to bootstrapServers)
                .plus(propertiesMap)
                .plus(propertiesFile?.asPropertiesMap.orEmpty())

        override fun open(): KafkaCluster = KafkaCluster(configMap, pollDuration, coroutineContext)
    }

    private inner class KafkaLog(
        private val clusterAlias: LogClusterAlias,
        private val topic: String,
        override val epoch: Int,
        private val groupId: String?
    ) : Log {

        private fun readLatestSubmittedMessage(kafkaConfigMap: KafkaConfigMap): LogOffset =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                (c.endOffsets(listOf(tp))[tp] ?: 0) - 1
            }

        private val latestSubmittedOffset0 = AtomicLong(readLatestSubmittedMessage(kafkaConfigMap))
        override val latestSubmittedOffset get() = latestSubmittedOffset0.get()

        override fun appendMessage(message: Message): CompletableFuture<MessageMetadata> =
            scope.future {
                CompletableDeferred<MessageMetadata>()
                    .also { res ->
                        producer.send(
                            ProducerRecord(topic, null, Unit, message.encode())
                        ) { recordMetadata, e ->
                            if (e == null) res.complete(
                                MessageMetadata(
                                    recordMetadata.offset(),
                                    Instant.ofEpochMilli(recordMetadata.timestamp())
                                )
                            ) else res.completeExceptionally(e)
                        }
                    }
                    .await()
                    .also { messageMetadata -> latestSubmittedOffset0.updateAndGet { it.coerceAtLeast(messageMetadata.logOffset) } }
            }

        override fun readLastMessage(): Message? =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                val lastOffset = c.endOffsets(listOf(tp))[tp]?.minus(1)?.takeIf { it >= 0 } ?: return null
                c.assign(listOf(tp))
                c.seek(tp, lastOffset)

                val records = c.poll(pollDuration).records(topic)
                records.firstOrNull()?.let { record -> Message.parse(record.value()) }
            }

        override fun openAtomicProducer(transactionalId: String) = object : AtomicProducer {
            private val producer = KafkaProducer(
                mapOf(
                    "enable.idempotence" to "true",
                    "acks" to "all",
                    "compression.type" to "snappy",
                    "transactional.id" to transactionalId,
                ) + kafkaConfigMap,
                UnitSerializer,
                ByteArraySerializer()
            ).also { it.initTransactions() }

            override fun openTx(): AtomicProducer.Tx {
                producer.beginTransaction()

                return object : AtomicProducer.Tx {
                    private val futures = mutableListOf<CompletableFuture<MessageMetadata>>()
                    private var isOpen = true

                    override fun appendMessage(message: Message): CompletableFuture<MessageMetadata> {
                        check(isOpen) { "Transaction already closed" }
                        val future = CompletableFuture<MessageMetadata>()
                        futures.add(future)
                        producer.send(ProducerRecord(topic, null, Unit, message.encode())) { recordMetadata, e ->
                            if (e == null) {
                                future.complete(
                                    MessageMetadata(
                                        recordMetadata.offset(),
                                        Instant.ofEpochMilli(recordMetadata.timestamp())
                                    )
                                )
                            } else {
                                future.completeExceptionally(e)
                            }
                        }
                        return future
                    }

                    override fun commit() {
                        check(isOpen) { "Transaction already closed" }
                        isOpen = false
                        // commitTransaction flushes all pending sends, so futures are already complete
                        producer.commitTransaction()
                        futures.forEach {
                            latestSubmittedOffset0.updateAndGet { prev -> prev.coerceAtLeast(it.join().logOffset) }
                        }
                    }

                    override fun abort() {
                        check(isOpen) { "Transaction already closed" }
                        isOpen = false
                        producer.abortTransaction()
                    }

                    override fun close() {
                        if (isOpen) abort()
                    }
                }
            }

            override fun close() {
                producer.close()
            }
        }

        override fun tailAll(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription {
            val job = scope.launch {
                kafkaConfigMap.openConsumer().use { c ->
                    TopicPartition(topic, 0).also { tp ->
                        c.assign(listOf(tp))
                        c.seek(tp, latestProcessedOffset + 1)
                    }

                    runInterruptible(Dispatchers.IO) {
                        while (true) {
                            val records = try {
                                c.poll(pollDuration).records(topic)
                            } catch (_: InterruptException) {
                                throw InterruptedException()
                            }

                            subscriber.processRecords(
                                records.mapNotNull { record ->
                                    Message.parse(record.value())
                                        ?.let { msg ->
                                            Record(
                                                record.offset(),
                                                Instant.ofEpochMilli(record.timestamp()),
                                                msg
                                            )
                                        }
                                }
                            )
                        }
                    }
                }
            }

            return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
        }

        override fun subscribe(subscriber: Subscriber, listener: AssignmentListener): Subscription {
            val groupId = requireNotNull(groupId) { "groupId must be configured to use subscribe" }

            val consumerConfig = kafkaConfigMap + mapOf("group.id" to groupId)

            val job = scope.launch {
                consumerConfig.openConsumer().use { consumer ->
                    val rebalanceListener = object : ConsumerRebalanceListener {
                        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                            listener.onPartitionsRevoked(partitions.map { it.partition() })
                        }

                        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                            val offsets = listener.onPartitionsAssigned(partitions.map { it.partition() })
                            for ((partition, offset) in offsets) {
                                consumer.seek(TopicPartition(topic, partition), offset)
                            }
                        }

                        override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
                            listener.onPartitionsLost(partitions.map { it.partition() })
                        }
                    }

                    consumer.subscribe(listOf(topic), rebalanceListener)

                    runInterruptible(Dispatchers.IO) {
                        while (true) {
                            val records = try {
                                consumer.poll(pollDuration).records(topic)
                            } catch (_: InterruptException) {
                                throw InterruptedException()
                            }

                            subscriber.processRecords(
                                records.mapNotNull { record ->
                                    Message.parse(record.value())
                                        ?.let { msg ->
                                            Record(
                                                record.offset(),
                                                Instant.ofEpochMilli(record.timestamp()),
                                                msg
                                            )
                                        }
                                }
                            )
                        }
                    }
                }
            }

            return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
        }

        override fun close() = Unit
    }

    @Serializable
    @SerialName("!Kafka")
    data class LogFactory @JvmOverloads constructor(
        val cluster: LogClusterAlias,
        val topic: String,
        var autoCreateTopic: Boolean = true,
        var epoch: Int = 0,
        var groupId: String? = null
    ) : Factory {

        fun autoCreateTopic(autoCreateTopic: Boolean) = apply { this.autoCreateTopic = autoCreateTopic }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }
        fun groupId(groupId: String) = apply { this.groupId = groupId }

        override fun openLog(clusters: Map<LogClusterAlias, Cluster>): Log {
            val clusterAlias = this.cluster
            val cluster = requireNotNull(clusters[clusterAlias] as? KafkaCluster) {
                "missing Kafka cluster: '$clusterAlias'"
            }

            val configMap = cluster.kafkaConfigMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(topic, autoCreateTopic)
            }

            return cluster.KafkaLog(clusterAlias, topic, epoch, groupId)
        }

        override fun openReadOnlyLog(clusters: Map<LogClusterAlias, Cluster>) =
            ReadOnlyLog(openLog(clusters))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.setOtherLog(ProtoAny.pack(kafkaLogConfig {
                this.topic = this@LogFactory.topic
                this.epoch = this@LogFactory.epoch
                this.logClusterAlias = cluster
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
                LogFactory(it.logClusterAlias, it.topic)
            }

        override fun registerSerde(builder: PolymorphicModuleBuilder<Log.Factory>) {
            builder.subclass(LogFactory::class)
        }
    }

    /**
     * @suppress
     */
    class ClusterRegistration : Log.Cluster.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Log.Cluster.Factory<*>>) {
            builder.subclass(ClusterFactory::class)
        }
    }
}
