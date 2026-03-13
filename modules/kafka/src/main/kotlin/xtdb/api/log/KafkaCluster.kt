@file:UseSerializers(
    StringMapWithEnvVarsSerde::class,
    DurationSerde::class,
    StringWithEnvVarSerde::class,
    PathWithEnvVarSerde::class
)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
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
import xtdb.database.proto.DatabaseConfig
import xtdb.kafka.proto.KafkaLogConfig
import xtdb.kafka.proto.kafkaLogConfig
import xtdb.util.MsgIdUtil.afterMsgIdToOffset
import xtdb.util.closeOnCatch
import java.nio.file.Path
import java.time.Duration
import java.time.Instant.ofEpochMilli
import java.util.*
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

private sealed interface ConsumerControl<out M> {
    data class TailAll<M>(val afterMsgId: MessageId, val processor: Log.RecordProcessor<M>) : ConsumerControl<M>
    data class StopTailing<M>(val onStop: CompletableDeferred<Unit>) : ConsumerControl<M>
}

class KafkaCluster(
    val kafkaConfigMap: KafkaConfigMap,
    private val pollDuration: Duration,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Log.Cluster {
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
    ) : Log.Cluster.Factory<KafkaCluster> {

        fun pollDuration(pollDuration: Duration) = apply { this.pollDuration = pollDuration }
        fun propertiesMap(propertiesMap: Map<String, String>) = apply { this.propertiesMap = propertiesMap }
        fun propertiesFile(propertiesFile: Path) = apply { this.propertiesFile = propertiesFile }

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

    private inner class KafkaLog<M>(
        private val codec: MessageCodec<M>,
        private val topic: String,
        override val epoch: Int,
        private val groupId: String
    ) : Log<M> {

        private fun readLatestSubmittedMessage(kafkaConfigMap: KafkaConfigMap): LogOffset =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                (c.endOffsets(listOf(tp))[tp] ?: 0) - 1
            }

        private val latestSubmittedOffset0 = AtomicLong(readLatestSubmittedMessage(kafkaConfigMap))
        override val latestSubmittedOffset get() = latestSubmittedOffset0.get()

        override fun appendMessage(message: M): Deferred<Log.MessageMetadata> =
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

        override fun readLastMessage(): M? =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                val lastOffset = c.endOffsets(listOf(tp))[tp]?.minus(1)?.takeIf { it >= 0 } ?: return null
                c.assign(listOf(tp))
                c.seek(tp, lastOffset)

                val records = c.poll(pollDuration).records(topic)
                records.firstOrNull()?.let { record -> codec.decode(record.value()) }
            }

        override fun openAtomicProducer(transactionalId: String) = object : Log.AtomicProducer<M> {
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

            override fun openTx(): Log.AtomicProducer.Tx<M> {
                producer.beginTransaction()

                return object : Log.AtomicProducer.Tx<M> {
                    private val futures = mutableListOf<CompletableDeferred<Log.MessageMetadata>>()
                    private var isOpen = true

                    override fun appendMessage(message: M): CompletableDeferred<Log.MessageMetadata> {
                        check(isOpen) { "Transaction already closed" }
                        val deferred = CompletableDeferred<Log.MessageMetadata>()
                        futures.add(deferred)
                        producer.send(ProducerRecord(topic, null, Unit, codec.encode(message))) { recordMetadata, e ->
                            if (e == null) {
                                deferred.complete(
                                    Log.MessageMetadata(
                                        epoch,
                                        recordMetadata.offset(),
                                        ofEpochMilli(recordMetadata.timestamp())
                                    )
                                )
                            } else {
                                deferred.completeExceptionally(e)
                            }
                        }
                        return deferred
                    }

                    @OptIn(ExperimentalCoroutinesApi::class)
                    override fun commit() {
                        check(isOpen) { "Transaction already closed" }
                        isOpen = false
                        // commitTransaction flushes all pending sends, so deferreds are already complete
                        producer.commitTransaction()
                        futures.forEach {
                            latestSubmittedOffset0.updateAndGet { prev -> prev.coerceAtLeast(it.getCompleted().logOffset) }
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

        @OptIn(ExperimentalCoroutinesApi::class)
        private inner class PollingConsumer(private val c: KafkaConsumer<*, ByteArray>) : Log.Consumer<M> {

            private val tp = TopicPartition(topic, 0)
            private val controlChannel = Channel<ConsumerControl<M>>()
            private var currentProcessor: Log.RecordProcessor<M>? = null

            private fun handleControl(msg: ConsumerControl<M>) {
                when (msg) {
                    is ConsumerControl.TailAll -> {
                        currentProcessor = msg.processor
                        val afterOffset = afterMsgIdToOffset(epoch, msg.afterMsgId)
                        c.seek(tp, afterOffset + 1)
                        c.resume(listOf(tp))
                    }

                    is ConsumerControl.StopTailing -> {
                        c.pause(listOf(tp))
                        currentProcessor = null
                        msg.onStop.complete(Unit)
                    }
                }
            }

            private val pollingJob = scope.launch(Dispatchers.IO) {
                while (isActive) {
                    select {
                        controlChannel.onReceive { msg -> handleControl(msg) }

                        onTimeout(0) {
                            val records = runInterruptible {
                                try {
                                    c.poll(pollDuration).records(topic)
                                        .mapNotNull { rec ->
                                            val msg = codec.decode(rec.value()) ?: return@mapNotNull null

                                            Log.Record(epoch, rec.offset(), ofEpochMilli(rec.timestamp()), msg)
                                        }
                                } catch (_: InterruptException) {
                                    throw InterruptedException()
                                }
                            }

                            currentProcessor?.processRecords(records)
                        }
                    }
                }
            }

            override fun tailAll(afterMsgId: MessageId, processor: Log.RecordProcessor<M>) =
                runBlocking {
                    controlChannel.send(ConsumerControl.TailAll(afterMsgId, processor))

                    Log.Subscription {
                        runBlocking {
                            val ack = CompletableDeferred<Unit>()
                            controlChannel.send(ConsumerControl.StopTailing(ack))
                            ack.await()
                        }
                    }
                }

            override fun close() {
                runBlocking {
                    withTimeout(30.seconds) { pollingJob.cancelAndJoin() }
                    c.close()
                }
            }
        }

        override fun openConsumer(): Log.Consumer<M> =
            kafkaConfigMap.openConsumer().closeOnCatch { c ->
                val tp = listOf(TopicPartition(topic, 0))
                c.assign(tp)
                c.pause(tp)

                PollingConsumer(c)
            }

        override fun openGroupConsumer(listener: Log.SubscriptionListener): Log.Consumer<M> =
            kafkaConfigMap.plus(mapOf("group.id" to groupId))
                .openConsumer()
                .closeOnCatch { c ->
                    c.subscribe(listOf(topic), object : ConsumerRebalanceListener {
                        // onPartitionsAssigned is called from the poll thread, so pause is safe here
                        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                            c.pause(partitions)
                            listener.onPartitionsAssignedSync(partitions.map { it.partition() })
                        }

                        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) =
                            listener.onPartitionsRevokedSync(partitions.map { it.partition() })

                        override fun onPartitionsLost(partitions: Collection<TopicPartition>) =
                            listener.onPartitionsLostSync(partitions.map { it.partition() })
                    })

                    PollingConsumer(c)
                }

        override fun close() = Unit
    }

    @Serializable
    @SerialName("!Kafka")
    data class LogFactory @JvmOverloads constructor(
        val cluster: LogClusterAlias,
        val topic: String,
        var replicaCluster: LogClusterAlias = cluster,
        var replicaTopic: String = "$topic-replica",
        var autoCreateTopic: Boolean = true,
        var epoch: Int = 0,
        var groupId: String = "xtdb-$topic"
    ) : Log.Factory {

        fun replicaCluster(replicaCluster: LogClusterAlias) = apply { this.replicaCluster = replicaCluster }
        fun replicaTopic(replicaTopic: String) = apply { this.replicaTopic = replicaTopic }
        fun autoCreateTopic(autoCreateTopic: Boolean) = apply { this.autoCreateTopic = autoCreateTopic }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }
        fun groupId(groupId: String) = apply { this.groupId = groupId }

        override fun openSourceLog(clusters: Map<LogClusterAlias, Log.Cluster>): Log<SourceMessage> {
            val clusterAlias = this.cluster
            val cluster = requireNotNull(clusters[clusterAlias] as? KafkaCluster) {
                "missing Kafka cluster: '$clusterAlias'"
            }

            val configMap = cluster.kafkaConfigMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(topic, autoCreateTopic)
            }

            return cluster.KafkaLog(SourceMessage.Codec, topic, epoch, groupId)
        }

        override fun openReadOnlySourceLog(clusters: Map<LogClusterAlias, Log.Cluster>) =
            ReadOnlyLog(openSourceLog(clusters))

        override fun openReplicaLog(clusters: Map<LogClusterAlias, Log.Cluster>): Log<ReplicaMessage> {
            val clusterAlias = this.replicaCluster
            val cluster = requireNotNull(clusters[clusterAlias] as? KafkaCluster) {
                "missing Kafka cluster: '$clusterAlias'"
            }

            val configMap = cluster.kafkaConfigMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(replicaTopic, autoCreateTopic)
            }

            return cluster.KafkaLog(ReplicaMessage.Codec, replicaTopic, epoch, groupId)
        }

        override fun openReadOnlyReplicaLog(clusters: Map<LogClusterAlias, Log.Cluster>) =
            ReadOnlyLog(openReplicaLog(clusters))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.setOtherLog(ProtoAny.pack(kafkaLogConfig {
                this.topic = this@LogFactory.topic
                this.epoch = this@LogFactory.epoch
                this.logClusterAlias = cluster
                this.replicaClusterAlias = replicaCluster
                this.replicaTopic = this@LogFactory.replicaTopic
                this.groupId = this@LogFactory.groupId
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
                    if (it.hasGroupId()) groupId = it.groupId
                }
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
