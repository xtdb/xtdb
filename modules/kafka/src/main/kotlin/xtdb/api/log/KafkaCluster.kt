@file:UseSerializers(
    DurationSerde::class,
    PathSerde::class
)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import xtdb.DurationSerde
import xtdb.api.PathSerde
import xtdb.database.proto.DatabaseConfig
import xtdb.kafka.proto.KafkaLogConfig
import xtdb.kafka.proto.kafkaLogConfig
import xtdb.util.MsgIdUtil.afterMsgIdToOffset
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.close
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import java.nio.file.Path
import java.time.Duration
import java.time.Instant.ofEpochMilli
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.io.path.inputStream
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import com.google.protobuf.Any as ProtoAny

private val LOG = KafkaCluster::class.logger

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
            "partition.assignment.strategy" to "org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
        ) + this,
        UnitDeserializer,
        ByteArrayDeserializer()
    )

fun AdminClient.ensureTopicExists(topic: String, autoCreate: Boolean) {
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
    val schemaRegistryUrl: String? = null,
    val transactionalIdPrefix: String? = null,
    private val groupId: String = "xtdb",
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Log.Cluster {
    val producer = kafkaConfigMap.openProducer()
    val scope = CoroutineScope(SupervisorJob() + coroutineContext)

    private sealed interface GroupCommand {
        class Register(val topic: String, val subscription: SharedGroupConsumer.TopicSubscription<*>) : GroupCommand
        class Unregister(val topic: String, val cont: CancellableContinuation<Unit>) : GroupCommand
    }

    @Suppress("UNCHECKED_CAST")
    private inner class SharedGroupConsumer : AutoCloseable {
        private val subscriptions = mutableMapOf<String, TopicSubscription<*>>()
        private val commandCh = Channel<GroupCommand>(Channel.UNLIMITED)

        private val consumer: KafkaConsumer<Unit, ByteArray> =
            kafkaConfigMap.plus(mapOf("group.id" to groupId)).openConsumer()

        inner class TopicSubscription<M>(
            val codec: MessageCodec<M>,
            val epoch: Int,
            val listener: Log.SubscriptionListener<M>,
            var processor: Log.RecordProcessor<M>?,
        ) {
            // expecting a load of change here for multi-part.

            suspend fun onPartitionAssigned(tp: TopicPartition) {
                listener.onPartitionsAssigned(listOf(tp.partition()))
                    ?.let { tailSpec ->
                        processor = tailSpec.processor
                        consumer.seek(tp, afterMsgIdToOffset(epoch, tailSpec.afterMsgId) + 1)
                    }
            }

            suspend fun onPartitionRevoked(tp: TopicPartition) {
                listener.onPartitionsRevoked(listOf(tp.partition()))
                processor = null
            }

            suspend fun processRecords(records: List<ConsumerRecord<*, ByteArray>>) {
                processor!!.processRecords(
                    records.mapNotNull { rec ->
                        codec.decode(rec.value())
                            ?.let { msg -> Log.Record(epoch, rec.offset(), ofEpochMilli(rec.timestamp()), msg) }
                    })
            }
        }


        private inline fun launderInterruptedException(block: () -> Unit) =
            try {
                block()
            } catch (e: InterruptedException) {
                throw InterruptException(e)
            }


        private fun applySubscriptions() {
            val topics = subscriptions.keys.toList()

            if (topics.isEmpty()) consumer.unsubscribe()
            else consumer.subscribe(topics, object : ConsumerRebalanceListener {
                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) =
                    launderInterruptedException {
                        runBlocking {
                            for ((topic, tps) in partitions.groupBy { it.topic() }) {
                                val sub = subscriptions[topic] as? TopicSubscription<Any?> ?: continue
                                sub.onPartitionAssigned(tps.single())
                            }
                        }
                    }

                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) =
                    launderInterruptedException {
                        runBlocking {
                            for ((topic, tps) in partitions.groupBy { it.topic() }) {
                                subscriptions[topic]?.onPartitionRevoked(tps.single())
                            }
                        }
                    }
            })
        }

        private fun processCommand(cmd: GroupCommand) {
            when (cmd) {
                is GroupCommand.Register -> {
                    check(cmd.topic !in subscriptions) { "Topic ${cmd.topic} already registered" }
                    subscriptions[cmd.topic] = cmd.subscription
                    applySubscriptions()
                }

                is GroupCommand.Unregister -> {
                    subscriptions.remove(cmd.topic)?.listener?.onPartitionsRevokedSync(listOf(0))
                    applySubscriptions()
                    cmd.cont.resume(Unit)
                }
            }
        }

        private suspend fun KafkaConsumer<*, ByteArray>.pollRecords() =
            runInterruptible(Dispatchers.IO) {
                try {
                    poll(pollDuration)
                } catch (_: WakeupException) {
                    ConsumerRecords.empty()
                } catch (e: InterruptException) {
                    throw InterruptedException().initCause(e)
                }
            }

        private val pollingJob: Job = scope.launch {
            try {
                while (isActive) {
                    select {
                        commandCh.onReceive { processCommand(it) }

                        if (subscriptions.isNotEmpty()) {
                            @OptIn(ExperimentalCoroutinesApi::class)
                            onTimeout(0.milliseconds) {
                                consumer.pollRecords()?.let { consumerRecords ->
                                    for ((topic, recs) in consumerRecords.groupBy { it.topic() }) {
                                        val sub = subscriptions[topic]
                                            ?: error("Received records for unsubscribed topic $topic")

                                        sub.processRecords(recs)
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                LOG.error(e) { "SharedGroupConsumer poll loop failed" }
                throw e
            }
        }

        suspend fun <M> register(
            topic: String,
            codec: MessageCodec<M>,
            epoch: Int,
            listener: Log.SubscriptionListener<M>
        ) {
            commandCh.send(GroupCommand.Register(topic, TopicSubscription(codec, epoch, listener, null)))
            consumer.wakeup()
        }

        suspend fun unregister(topic: String) {
            suspendCancellableCoroutine { cont ->
                commandCh.trySendBlocking(GroupCommand.Unregister(topic, cont)).getOrThrow()
                consumer.wakeup()
            }
        }

        override fun close() {
            consumer.wakeup()
            pollingJob.cancel()
            commandCh.close()
            try {
                runBlocking { withTimeout(30.seconds) { pollingJob.join() } }
            } finally {
                check(subscriptions.isEmpty()) { "SharedGroupConsumer closed with active subscriptions: ${subscriptions.keys}" }
                consumer.close()
            }
        }
    }

    private val _sharedGroupConsumer = lazy { SharedGroupConsumer() }
    private val sharedGroupConsumer by _sharedGroupConsumer

    override fun close() {
        try {
            _sharedGroupConsumer.close()
        } finally {
            runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
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
        var transactionalIdPrefix: String? = null,
        var groupId: String = "xtdb",
        @kotlinx.serialization.Transient var coroutineContext: CoroutineContext = Dispatchers.Default
    ) : Log.Cluster.Factory<KafkaCluster> {

        fun pollDuration(pollDuration: Duration) = apply { this.pollDuration = pollDuration }
        fun propertiesMap(propertiesMap: Map<String, String>) = apply { this.propertiesMap = propertiesMap }
        fun propertiesFile(propertiesFile: Path) = apply { this.propertiesFile = propertiesFile }
        fun schemaRegistryUrl(schemaRegistryUrl: String) = apply { this.schemaRegistryUrl = schemaRegistryUrl }
        fun transactionalIdPrefix(transactionalIdPrefix: String?) =
            apply { this.transactionalIdPrefix = transactionalIdPrefix }

        fun groupId(groupId: String) = apply { this.groupId = groupId }

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
            KafkaCluster(configMap, pollDuration, schemaRegistryUrl, transactionalIdPrefix, groupId, coroutineContext)
    }

    interface AtomicProducer<M> : Log.AtomicProducer<M> {
        override fun openTx(): Tx<M>

        interface Tx<M> : Log.AtomicProducer.Tx<M> {
            fun sendOffsetsToTransaction(
                offsets: Map<TopicPartition, OffsetAndMetadata>,
                groupMetadata: ConsumerGroupMetadata,
            )
        }

        companion object {
            inline fun <M, R> AtomicProducer<M>.withTx(block: (Tx<M>) -> R): R =
                openTx().use { tx ->
                    try {
                        block(tx).also { tx.commit() }
                    } catch (e: Throwable) {
                        try {
                            tx.abort()
                        } catch (abortEx: Throwable) {
                            e.addSuppressed(abortEx)
                        }
                        throw e
                    }
                }
        }
    }

    private inner class KafkaLog<M>(
        private val codec: MessageCodec<M>,
        private val topic: String,
        override val epoch: Int,
    ) : Log<M> {

        private fun readLatestSubmittedMessage(kafkaConfigMap: KafkaConfigMap): LogOffset =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                (c.endOffsets(listOf(tp))[tp] ?: 0) - 1
            }

        private val latestSubmittedOffset0 = AtomicLong(readLatestSubmittedMessage(kafkaConfigMap))
        override val latestSubmittedOffset get() = latestSubmittedOffset0.get()

        override suspend fun appendMessage(message: M): Log.MessageMetadata =
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

        override fun readLastMessage(): M? =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                val lastOffset = c.endOffsets(listOf(tp))[tp]?.minus(1)?.takeIf { it >= 0 } ?: return null
                c.assign(listOf(tp))
                c.seek(tp, lastOffset)

                val records = c.poll(pollDuration).records(topic)
                records.firstOrNull()?.let { record -> codec.decode(record.value()) }
            }

        override fun readRecords(fromMsgId: MessageId, toMsgId: MessageId) = sequence {
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

        override fun openAtomicProducer(transactionalId: String) = object : AtomicProducer<M> {
            private val prefixedTxId = listOfNotNull(transactionalIdPrefix, transactionalId).joinToString("-")

            init {
                LOG.info { "starting atomic producer with transactional.id '$prefixedTxId'" }
            }

            private val producer = KafkaProducer(
                mapOf(
                    "enable.idempotence" to "true",
                    "acks" to "all",
                    "compression.type" to "snappy",
                    "transactional.id" to prefixedTxId,
                ) + kafkaConfigMap,
                UnitSerializer,
                ByteArraySerializer()
            ).also { it.initTransactions() }

            override fun openTx(): AtomicProducer.Tx<M> {
                producer.beginTransaction()

                return object : AtomicProducer.Tx<M> {
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

                    override fun sendOffsetsToTransaction(
                        offsets: Map<TopicPartition, OffsetAndMetadata>,
                        groupMetadata: ConsumerGroupMetadata,
                    ) {
                        check(isOpen) { "Transaction already closed" }
                        producer.sendOffsetsToTransaction(offsets, groupMetadata)
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

        override suspend fun tailAll(afterMsgId: MessageId, processor: Log.RecordProcessor<M>) = coroutineScope {
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                c.assign(listOf(tp))
                c.seek(tp, afterMsgIdToOffset(epoch, afterMsgId) + 1)

                while (isActive) {
                    val records = runInterruptible(Dispatchers.IO) { c.pollRecords() }
                    if (records.isNotEmpty()) processor.processRecords(records)
                }
            }
        }

        override suspend fun openGroupSubscription(listener: Log.SubscriptionListener<M>) {
            sharedGroupConsumer.register(topic, codec, epoch, listener)
            LOG.info { "registered group subscription for topic '$topic'" }
            try {
                suspendCancellableCoroutine<Unit> { } // wait until cancelled
            } finally {
                withContext(NonCancellable) { sharedGroupConsumer.unregister(topic) }
            }
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
    ) : Log.Factory {

        fun replicaCluster(replicaCluster: LogClusterAlias) = apply { this.replicaCluster = replicaCluster }
        fun replicaTopic(replicaTopic: String) = apply { this.replicaTopic = replicaTopic }
        fun autoCreateTopic(autoCreateTopic: Boolean) = apply { this.autoCreateTopic = autoCreateTopic }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openSourceLog(clusters: Map<LogClusterAlias, Log.Cluster>): Log<SourceMessage> {
            val clusterAlias = this.cluster
            val cluster = requireNotNull(clusters[clusterAlias] as? KafkaCluster) {
                "missing Kafka cluster: '$clusterAlias'"
            }

            val configMap = cluster.kafkaConfigMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(topic, autoCreateTopic)
            }

            return cluster.KafkaLog(SourceMessage.Codec, topic, epoch)
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

            return cluster.KafkaLog(ReplicaMessage.Codec, replicaTopic, epoch)
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
    class ClusterRegistration : Log.Cluster.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Log.Cluster.Factory<*>>) {
            builder.subclass(ClusterFactory::class)
        }
    }
}
