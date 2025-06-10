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
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteBufferSerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringMapWithEnvVarsSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.Xtdb
import xtdb.api.log.Log.*
import xtdb.api.module.XtdbModule
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.inputStream
import kotlin.time.Duration.Companion.seconds

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
        ByteBufferSerializer()
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
            } catch (e: UnknownTopicOrPartitionException) {
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

class KafkaLog internal constructor(
    private val kafkaConfigMap: KafkaConfigMap,
    private val topic: String,
    private val pollDuration: Duration,
    override val epoch: Int
) : Log {

    private val producer = kafkaConfigMap.openProducer()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        producer.close()
    }

    private fun readLatestSubmittedMessage(kafkaConfigMap: KafkaConfigMap): LogOffset =
        kafkaConfigMap.openConsumer().use { c ->
            val tp = TopicPartition(topic, 0)
            (c.endOffsets(listOf(tp))[tp] ?: 0).toLong() - 1
        }

    private val latestSubmittedOffset0 = AtomicLong(readLatestSubmittedMessage(kafkaConfigMap))
    override val latestSubmittedOffset get() = latestSubmittedOffset0.get()

    override fun appendMessage(message: Message): CompletableFuture<MessageMetadata> =
        scope.future {
            CompletableDeferred<MessageMetadata>()
                .also { res ->
                    producer.send(
                        ProducerRecord(topic, null, Unit, message.encode()),
                        Callback { recordMetadata, e ->
                            if (e == null) res.complete(MessageMetadata(recordMetadata.offset(), Instant.ofEpochMilli(recordMetadata.timestamp()))) else res.completeExceptionally(e)
                        }
                    )
                }
                .await()
                .also { messageMetadata -> latestSubmittedOffset0.updateAndGet { it.coerceAtLeast(messageMetadata.logOffset) } }
        }

    override fun subscribe(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription {
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
                        } catch (e: InterruptException) {
                            throw InterruptedException()
                        }

                        subscriber.processRecords(
                            records.map { record ->
                                Record(
                                    record.offset(),
                                    Instant.ofEpochMilli(record.timestamp()),
                                    Message.parse(ByteBuffer.wrap(record.value()))
                                )
                            }
                        )
                    }
                }
            }
        }

        return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
    }

    companion object {
        @JvmStatic
        fun kafka(bootstrapServers: String, topic: String) =
            Factory(bootstrapServers, topic)

        @JvmSynthetic
        fun Xtdb.Config.kafka(
            bootstrapServers: String,
            topic: String,
            configure: Factory.() -> Unit = {}
        ) {
            log = KafkaLog.kafka(bootstrapServers, topic).also(configure)
        }
    }

    /**
     * Used to set configuration options for Kafka as an XTDB Log.
     *
     * For more info on setting up the necessary infrastructure to be able to use Kafka as an XTDB Log, see the
     * section on infrastructure within our [Kafka Module Reference](https://docs.xtdb.com/config/log/kafka.html).
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    log = KafkaLogFactory(
     *            bootstrapServers = "localhost:9092",
     *            topic = "xtdb_txs",
     *            autoCreateTopics = true,
     *            pollDuration = Duration.ofSeconds(1)
     *          ),
     *    ...
     * }
     * ```
     *
     * @property bootstrapServers A comma-separated list of host:port pairs to use for establishing the initial connection to the Kafka cluster.
     * @property topic Name of the Kafka topic to use for the log.
     * @property autoCreateTopic Whether to automatically create the topics, if it does not already exist.
     * @property pollDuration The maximum amount of time to block waiting for records to be returned by the Kafka consumer.
     * @property propertiesMap A map of Kafka connection properties, supplied directly to the Kafka client.
     * @property propertiesFile Path to a Java properties file containing Kafka connection properties, supplied directly to the Kafka client.
     */
    @Serializable
    @SerialName("!Kafka")
    data class Factory(
        val bootstrapServers: String,
        val topic: String,
        var autoCreateTopic: Boolean = true,
        var pollDuration: Duration = Duration.ofSeconds(1),
        var propertiesMap: Map<String, String> = emptyMap(),
        var propertiesFile: Path? = null,
        var epoch: Int = 0
    ) : Log.Factory {

        fun autoCreateTopic(autoCreateTopic: Boolean) = apply { this.autoCreateTopic = autoCreateTopic }
        fun pollDuration(pollDuration: Duration) = apply { this.pollDuration = pollDuration }
        fun propertiesMap(propertiesMap: Map<String, String>) = apply { this.propertiesMap = propertiesMap }
        fun propertiesFile(propertiesFile: Path) = apply { this.propertiesFile = propertiesFile }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        private val Path.asPropertiesMap: Map<String, String>
            get() =
                Properties()
                    .apply { load(inputStream()) }
                    .entries.associate { it.key as String to it.value as String }

        private val configMap: Map<String, String>
            get() = mapOf("bootstrap.servers" to bootstrapServers)
                .plus(propertiesMap)
                .plus(propertiesFile?.asPropertiesMap.orEmpty())

        override fun openLog(): KafkaLog {
            val configMap = this.configMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(topic, autoCreateTopic)
            }

            return KafkaLog(configMap, topic, pollDuration, epoch)
        }
    }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerLogFactory(Factory::class)
        }
    }
}

