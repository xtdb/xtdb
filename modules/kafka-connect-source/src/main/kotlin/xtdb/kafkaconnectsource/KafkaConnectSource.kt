package xtdb.kafkaconnectsource

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runInterruptible
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.subclass
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.connect.connector.ConnectRecord
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
import xtdb.kafkaconnectsource.proto.KafkaConnectSourceConfig
import xtdb.kafkaconnectsource.proto.KafkaConnectSourceToken
import xtdb.kafkaconnectsource.proto.kafkaConnectSourceConfig
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn
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

class KafkaConnectSource(
    private val dbName: String,
    private val cluster: KafkaCluster,
    private val topic: String,
    private val connectConfig: Map<String, String>,
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

            return KafkaConnectSource(dbName, cluster, topic, connectConfig, indexer.open(dbName))
        }

        class Registration : ExternalSource.Registration<Factory> {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.kafkaconnectsource.proto.KafkaConnectSourceConfig"

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
                builder.subclass(Factory::class)
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
            DEFAULT_CONSUMER_CONFIG + cluster.kafkaConfigMap + filteredConsumerConfig(connectConfig) + HARDCODED_CONSUMER_CONFIG

        try {
            openConverter(connectConfig, ConverterRole.KEY).use { keyConverter ->
                openConverter(connectConfig, ConverterRole.VALUE).use { valueConverter ->
                    SimpleHeaderConverter().also { it.configure(emptyMap<String, Any>()) }.use { headerConverter ->
                        openTransforms(connectConfig).use { transformChain ->
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

private enum class ConverterRole(val configKey: String, val isKey: Boolean) {
    KEY("key.converter", true),
    VALUE("value.converter", false),
}

private fun openConverter(connectConfig: Map<String, String>, role: ConverterRole): Converter {
    val className = connectConfig[role.configKey]
        ?: throw Incorrect(
            "missing '${role.configKey}' in connectConfig",
            "xtdb.kafka-connect-source/missing-converter",
            mapOf("role" to role.configKey),
        )
    val nested = connectConfig
        .filterKeys { it.startsWith("${role.configKey}.") }
        .mapKeys { (k, _) -> k.removePrefix("${role.configKey}.") }

    // NOTE: different from KC. KC's `Plugins.newConverter` uses an isolated classpath for
    // dependency isolation between plugins.
    val cls = try {
        Class.forName(className).asSubclass(Converter::class.java)
    } catch (e: Exception) {
        throw Incorrect(
            "couldn't load ${role.configKey} class '$className': ${e.message}",
            "xtdb.kafka-connect-source/converter-class-not-found",
            mapOf("role" to role.configKey, "className" to className),
            cause = e,
        )
    }
    return cls.getDeclaredConstructor().newInstance().apply { configure(nested, role.isKey) }
}

private fun filteredConsumerConfig(connectConfig: Map<String, String>): Map<String, String> =
    connectConfig.filterKeys {
        !it.startsWith("key.converter") &&
            !it.startsWith("value.converter") &&
            !it.startsWith("transforms") &&
            !it.startsWith("predicates")
    }

internal class TransformChain(
    private val transforms: List<Pair<Transformation<SinkRecord>, Predicate<SinkRecord>?>>,
) : AutoCloseable {
    fun apply(initial: SinkRecord): SinkRecord? {
        var rec: SinkRecord? = initial
        for ((transform, predicate) in transforms) {
            val current = rec ?: return null
            if (predicate == null || predicate.test(current)) {
                rec = transform.apply(current)
            }
        }
        return rec
    }

    override fun close() {
        for ((transform, predicate) in transforms) {
            runCatching { transform.close() }
            runCatching { predicate?.close() }
        }
    }
}

@Suppress("UNCHECKED_CAST")
private fun openTransforms(connectConfig: Map<String, String>): TransformChain {
    val raw = connectConfig["transforms"]?.trim().orEmpty()
    val aliases = if (raw.isEmpty()) emptyList()
        else raw.split(",").map { it.trim() }
    if (aliases.any { it.isEmpty() }) {
        throw Incorrect(
            "empty alias in 'transforms' — check for stray commas",
            "xtdb.kafka-connect-source/empty-transform-alias",
            mapOf("transforms" to raw),
        )
    }

    val chain = aliases.map { alias ->
        val typeKey = "transforms.$alias.type"
        val type = connectConfig[typeKey]
            ?: throw Incorrect(
                "missing '$typeKey' for transform alias '$alias'",
                "xtdb.kafka-connect-source/missing-transform-type",
                mapOf("alias" to alias),
            )
        val predicateAlias = connectConfig["transforms.$alias.predicate"]
        val negate = connectConfig["transforms.$alias.negate"]?.toBoolean() == true

        val transformConfig = connectConfig
            .filterKeys { it.startsWith("transforms.$alias.") }
            .filterKeys { !it.endsWith(".type") && !it.endsWith(".predicate") && !it.endsWith(".negate") }
            .mapKeys { (k, _) -> k.removePrefix("transforms.$alias.") }

        val transform = (Class.forName(type) as Class<Transformation<SinkRecord>>)
            .getDeclaredConstructor().newInstance()
            .apply { configure(transformConfig) }

        val predicate = predicateAlias?.let { openPredicate(connectConfig, it, negate) }

        transform to predicate
    }
    return TransformChain(chain)
}

@Suppress("UNCHECKED_CAST")
private fun openPredicate(
    connectConfig: Map<String, String>,
    alias: String,
    negate: Boolean,
): Predicate<SinkRecord> {
    val type = connectConfig["predicates.$alias.type"]
        ?: throw Incorrect(
            "missing 'predicates.$alias.type'",
            "xtdb.kafka-connect-source/missing-predicate-type",
            mapOf("alias" to alias),
        )
    val predicateConfig = connectConfig
        .filterKeys { it.startsWith("predicates.$alias.") && !it.endsWith(".type") }
        .mapKeys { (k, _) -> k.removePrefix("predicates.$alias.") }

    val base = (Class.forName(type) as Class<Predicate<SinkRecord>>)
        .getDeclaredConstructor().newInstance()
        .apply { configure(predicateConfig) }

    return if (negate) NegatedPredicate(base) else base
}

private class NegatedPredicate<R : ConnectRecord<R>>(
    private val inner: Predicate<R>,
) : Predicate<R> {
    override fun config() = inner.config()
    override fun test(record: R): Boolean = !inner.test(record)
    override fun close() = inner.close()
    override fun configure(configs: MutableMap<String, *>?) = inner.configure(configs)
}
