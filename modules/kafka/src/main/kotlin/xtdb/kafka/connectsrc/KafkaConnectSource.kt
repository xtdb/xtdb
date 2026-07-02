package xtdb.kafka.connectsrc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runInterruptible
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.storage.Converter
import org.apache.kafka.connect.storage.HeaderConverter
import org.apache.kafka.connect.storage.SimpleHeaderConverter
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.predicates.Predicate
import io.micrometer.core.instrument.MeterRegistry
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.KafkaCluster
import xtdb.database.ExternalSource
import xtdb.database.ExternalSourceToken
import xtdb.error.Incorrect
import xtdb.indexer.TxIndexer
import xtdb.kafka.connectsrc.proto.KafkaConnectSourceConfig
import xtdb.kafka.connectsrc.proto.KafkaConnectSourceToken
import xtdb.kafka.connectsrc.proto.kafkaConnectSourceConfig
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import java.time.Duration
import com.google.protobuf.Any as ProtoAny

private val LOG = KafkaConnectSource::class.logger

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"
private val POLL_DURATION: Duration = Duration.ofSeconds(1)

private val HARDCODED_CONSUMER_CONFIG: Map<String, String> = mapOf(
    "enable.auto.commit" to "false",
    "key.deserializer" to ByteArrayDeserializer::class.java.name,
    "value.deserializer" to ByteArrayDeserializer::class.java.name,
)

private val DEFAULT_CONSUMER_CONFIG: Map<String, String> = mapOf(
    "auto.offset.reset" to "none",
)

class KafkaConnectSource internal constructor(
    private val dbName: String,
    private val cluster: KafkaCluster,
    private val topic: String,
    private val connectConfig: ConnectConfig,
    private val indexer: RecordIndexer,
) : ExternalSource {

    @Serializable
    @SerialName("!KafkaConnect")
    data class Factory(
        val remote: RemoteAlias,
        val topic: String,
        val connectConfig: Map<String, String> = emptyMap(),
        val indexer: RecordIndexer.Factory,
    ) : ExternalSource.Factory {

        override fun open(
            dbName: String,
            remotes: Map<RemoteAlias, Remote>,
            meterRegistry: MeterRegistry?,
        ): ExternalSource {
            val raw = remotes[remote]
                ?: throw Incorrect(
                    "no remote configured with alias '$remote' — add a '!Kafka' entry under 'remotes:' in node config",
                    errorCode = "xtdb.kafka-connect-source/missing-remote",
                    data = mapOf("alias" to remote),
                )

            val actualType = raw::class.simpleName ?: raw::class.qualifiedName ?: "unknown"

            val cluster = raw as? KafkaCluster
                ?: throw Incorrect(
                    "remote '$remote' is a $actualType, expected a !Kafka remote",
                    errorCode = "xtdb.kafka-connect-source/wrong-remote-type",
                    data = mapOf("alias" to remote, "actualType" to actualType),
                )

            return KafkaConnectSource(dbName, cluster, topic, ConnectConfig.parse(connectConfig), indexer.open())
        }

        class Registration : ExternalSource.Registration<Factory> {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.kafka.connectsrc.proto.KafkaConnectSourceConfig"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(kafkaConnectSourceConfig {
                    remote = factory.remote
                    topic = factory.topic
                    connectConfig.putAll(factory.connectConfig)
                    indexer = RecordIndexer.Factory.toProto(factory.indexer)
                }, PROTO_TAG_PREFIX)

            override fun fromProto(msg: ProtoAny): Factory {
                val config = msg.unpack(KafkaConnectSourceConfig::class.java)
                return Factory(
                    remote = config.remote,
                    topic = config.topic,
                    connectConfig = config.connectConfigMap,
                    indexer = RecordIndexer.Factory.fromProto(config.indexer),
                )
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class, ValidatingFactorySerializer)
            }

            override val serializersModule: SerializersModule = RecordIndexer.Factory.serializersModule
        }
    }

    override suspend fun onPartitionAssigned(
        partition: Int,
        afterToken: ExternalSourceToken?,
        txIndexer: TxIndexer,
    ) {
        LOG.info("[$dbName] Partition $partition assigned (topic=$topic)")

        val mergedConsumerConfig: Map<String, Any> =
            DEFAULT_CONSUMER_CONFIG + cluster.kafkaConfigMap + HARDCODED_CONSUMER_CONFIG

        try {
            connectConfig.keyConverter.open().use { keyConverter ->
                connectConfig.valueConverter.open().use { valueConverter ->
                    SimpleHeaderConverter().also { it.configure(emptyMap<String, Any>()) }.use { headerConverter ->
                        connectConfig.openTransformChain().use { transformChain ->
                            KafkaConsumer<ByteArray?, ByteArray?>(mergedConsumerConfig).use { consumer ->
                                val tp = TopicPartition(topic, partition)
                                consumer.assign(listOf(tp))

                                if (afterToken != null) {
                                    val offset = KafkaConnectSourceToken.parseFrom(afterToken).offset + 1
                                    LOG.info("[$dbName] Resuming from offset $offset on $topic-$partition")
                                    consumer.seek(tp, offset)
                                } else {
                                    LOG.info("[$dbName] No token — seeking to beginning of $topic-$partition")
                                    consumer.seekToBeginning(listOf(tp))
                                }

                                while (currentCoroutineContext().isActive) {
                                    val records = try {
                                        runInterruptible(Dispatchers.IO) { consumer.poll(POLL_DURATION) }
                                    } catch (_: WakeupException) {
                                        break
                                    } catch (_: InterruptException) {
                                        break
                                    }

                                    if (records.isEmpty) continue

                                    // Halt-on-failure: converter / transform exceptions propagate up. Mirrors
                                    // Kafka Connect's `errors.tolerance=none` default. If we wanted DLQ-style
                                    // skip-and-continue (`errors.tolerance=all`), we'd need to interleave aborted
                                    // txs with indexer commits in offset order to keep the resume token correct.
                                    val sinkRecords = records.records(tp).mapNotNull { rec ->
                                        val sinkRec = buildSinkRecord(rec, keyConverter, valueConverter, headerConverter)
                                        transformChain.apply(sinkRec)
                                    }

                                    if (sinkRecords.isNotEmpty()) {
                                        indexer.indexRecords(sinkRecords, txIndexer)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            LOG.error(e, "[$dbName] External source failed")
            throw e
        }
    }

    private fun buildSinkRecord(
        rec: ConsumerRecord<ByteArray?, ByteArray?>,
        keyConverter: Converter,
        valueConverter: Converter,
        headerConverter: HeaderConverter,
    ): SinkRecord {
        val key: SchemaAndValue = keyConverter.toConnectData(rec.topic(), rec.key())
        val value: SchemaAndValue = valueConverter.toConnectData(rec.topic(), rec.value())
        val headers = convertHeaders(rec.headers(), rec.topic(), headerConverter)
        return SinkRecord(
            rec.topic(), rec.partition(),
            key.schema(), key.value(),
            value.schema(), value.value(),
            rec.offset(), rec.timestamp(), rec.timestampType(),
            headers,
        )
    }

    private fun convertHeaders(
        kafkaHeaders: Headers,
        topic: String,
        headerConverter: HeaderConverter,
    ): ConnectHeaders {
        val connectHeaders = ConnectHeaders()
        for (header in kafkaHeaders) {
            val sv = headerConverter.toConnectHeader(topic, header.key(), header.value())
            connectHeaders.add(header.key(), sv)
        }
        return connectHeaders
    }

    override fun close() {
        LOG.info("[$dbName] Closing external source")
        runCatching { indexer.close() }
            .onFailure { LOG.error(it, "[$dbName] Indexer close failed") }
    }
}

/**
 * The generated serializer plus a `ConnectConfig.parse` on decode, so a bad `connectConfig` fails the
 * `ATTACH DATABASE` statement (or node-config load) itself rather than the eventual leader transition.
 *
 * Deliberately YAML-side only: the kotlinx serde is only ever fed live operator input. The proto path
 * ([KafkaConnectSource.Factory.Registration.fromProto] — block-catalog reload, log replay) constructs the
 * Factory directly and MUST stay validation-free, so configs attached under older, looser rules remain
 * readable and `XTDB_SKIP_DBS` can still park them.
 */
private object ValidatingFactorySerializer : KSerializer<KafkaConnectSource.Factory> {
    private val delegate = KafkaConnectSource.Factory.serializer()
    override val descriptor get() = delegate.descriptor
    override fun serialize(encoder: Encoder, value: KafkaConnectSource.Factory) = delegate.serialize(encoder, value)
    override fun deserialize(decoder: Decoder): KafkaConnectSource.Factory =
        delegate.deserialize(decoder).also { ConnectConfig.parse(it.connectConfig) }
}

internal class TransformChain(
    private val steps: List<Step>,
    private val predicates: Collection<Predicate<SinkRecord>>,
) : AutoCloseable {
    data class Step(
        val transform: Transformation<SinkRecord>,
        val predicate: Predicate<SinkRecord>?,
        val negate: Boolean,
    )

    fun apply(initial: SinkRecord): SinkRecord? {
        var rec: SinkRecord? = initial
        for (step in steps) {
            val current = rec ?: return null
            if (step.predicate == null || step.predicate.test(current) != step.negate) {
                rec = step.transform.apply(current)
            }
        }
        return rec
    }

    override fun close() {
        // a predicate may be shared across steps, so close the distinct instances — not per-step
        steps.forEach { runCatching { it.transform.close() } }
        predicates.forEach { runCatching { it.close() } }
    }
}
