package xtdb.kafka.connectsrc

import kotlinx.coroutines.CancellationException
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.kafka.connect.data.Date as ConnectDate
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time as ConnectTime
import org.apache.kafka.connect.data.Timestamp as ConnectTimestamp
import org.apache.kafka.connect.sink.SinkRecord
import xtdb.api.error.Anomaly.Companion.toAnomaly
import xtdb.api.error.Incorrect
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.kafka.connectsrc.proto.DocsIndexerConfig
import xtdb.kafka.connectsrc.proto.docsIndexerConfig
import xtdb.kafka.connectsrc.proto.kafkaConnectSourceToken
import xtdb.api.TableRef
import xtdb.util.asIid
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneOffset
import com.google.protobuf.Any as ProtoAny

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

class DocsIndexer(internal val table: TableRef) : RecordIndexer {

    @Serializable
    @SerialName("!Docs")
    data class Factory(
        val table: String,
    ) : RecordIndexer.Factory {

        override fun open(): RecordIndexer =
            DocsIndexer(TableRef.parse(table))

        class Registration : RecordIndexer.Registration<Factory> {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.kafka.connectsrc.proto.DocsIndexerConfig"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(
                    docsIndexerConfig {
                        table = factory.table
                    },
                    PROTO_TAG_PREFIX,
                )

            override fun fromProto(msg: ProtoAny): Factory {
                val config = msg.unpack(DocsIndexerConfig::class.java)
                return Factory(table = config.table)
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<RecordIndexer.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }

    override suspend fun indexRecords(records: List<SinkRecord>, txIndexer: TxIndexer) {
        for (rec in records) {
            val token = kafkaConnectSourceToken { offset = rec.kafkaOffset() }.toByteArray()
            val systemTime = rec.timestamp()?.let { Instant.ofEpochMilli(it) }

            // Fire-and-forget: resume is anchored to XTDB's per-tx import token (the consumer re-seeks from
            // it on restart and never commits offsets back to Kafka), so a crash replays from the last
            // *imported* offset — handing off without awaiting the result can't lose or skip a record.
            txIndexer.submitTx(token, systemTime = systemTime) { openTx ->
                try {
                    writeRecord(openTx, rec)
                    TxResult.Committed()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    throw e.toAnomaly(coordsFor(rec))
                }
            }
        }
    }

    private fun writeRecord(openTx: OpenTx, rec: SinkRecord) {
        val openTxTable = openTx.table(table.schemaName, table.tableName)

        val id = resolveId(rec)
        val value = rec.value()

        if (value == null) {
            openTxTable.apply {
                writeId(id)
                writeDefaultValidTime()
                endDelete()
            }

            return
        }

        val converted = convertValue(value, rec.valueSchema())

        @Suppress("UNCHECKED_CAST")
        val docMap = (converted as? Map<String, Any?>)?.toMutableMap()
            ?: throw Incorrect(
                "expected Struct or Map at top level, got ${value::class.simpleName}",
                "xtdb.kafka-connect-source/docs-non-struct-value",
                mapOf(
                    "topic" to rec.topic(),
                    "partition" to rec.kafkaPartition(),
                    "offset" to rec.kafkaOffset(),
                    "valueType" to value::class.simpleName,
                ),
            )

        docMap["_id"] = id

        openTxTable.apply {
            writeId(id)
            writeDefaultValidTime()
            putDocWriter.writeObject(docMap)
            endPut()
        }
    }
}

private fun coordsFor(rec: SinkRecord): Map<String, Any?> =
    mapOf("topic" to rec.topic(), "partition" to rec.kafkaPartition(), "offset" to rec.kafkaOffset())

// Kafka's compaction, per-key ordering and tombstones all hang off the key, so it's the
// sole source of `_id` — deriving from the value would let upsert- and delete-identity diverge.
internal fun resolveId(rec: SinkRecord): Any {
    val id = rec.key()?.let { unwrapKeyForId(it) } ?: throw Incorrect(
        "record has no usable key — can't resolve _id (use the ValueToKey SMT to derive a key from the value)",
        "xtdb.kafka-connect-source/docs-no-key",
        coordsFor(rec),
    )

    // Probe rather than duplicate the whitelist, so the accepted key types can't drift from `asIid`.
    try {
        id.asIid
    } catch (e: Incorrect) {
        throw Incorrect(
            "record key of type ${id.javaClass.name} can't be used as _id (use the ExtractField SMT to extract a scalar key field)",
            "xtdb.kafka-connect-source/docs-invalid-key",
            coordsFor(rec) + ("keyType" to id.javaClass.name),
            e,
        )
    }

    return id
}

// AvroConverter et al may yield a single-field Struct as the key — unwrap to the inner value,
// which may itself be null (a nullable key field): the caller treats that as no usable key.
private fun unwrapKeyForId(key: Any): Any? =
    if (key is Struct && key.schema()?.fields()?.size == 1)
        key.get(key.schema().fields()[0])
    else key

@Suppress("UNCHECKED_CAST")
internal fun convertValue(v: Any?, schema: Schema?): Any? {
    if (v == null) return null

    if (schema != null) {
        when (schema.name()) {
            Decimal.LOGICAL_NAME -> return v as BigDecimal
            ConnectTimestamp.LOGICAL_NAME -> return (v as java.util.Date).toInstant()
            ConnectDate.LOGICAL_NAME -> return (v as java.util.Date).toInstant().atZone(ZoneOffset.UTC).toLocalDate()
            ConnectTime.LOGICAL_NAME -> return (v as java.util.Date).toInstant().atZone(ZoneOffset.UTC).toLocalTime()
        }
        return when (schema.type()) {
            Schema.Type.STRUCT -> structToMap(v as Struct)
            Schema.Type.ARRAY -> (v as List<Any?>).map { convertValue(it, schema.valueSchema()) }
            Schema.Type.MAP -> (v as Map<Any?, Any?>).entries
                .associateTo(mutableMapOf<String, Any?>()) { (k, vv) ->
                    k.toString() to convertValue(
                        vv,
                        schema.valueSchema()
                    )
                }

            Schema.Type.BYTES -> when (v) {
                is ByteBuffer -> ByteArray(v.remaining()).also { v.duplicate().get(it) }
                is ByteArray -> v
                else -> v
            }

            else -> v
        }
    }

    return when (v) {
        is Struct -> structToMap(v)
        is List<*> -> v.map { convertValue(it, null) }
        is Map<*, *> -> v.entries.associateTo(mutableMapOf<String, Any?>()) { (k, vv) ->
            k.toString() to convertValue(
                vv,
                null
            )
        }

        is ByteBuffer -> ByteArray(v.remaining()).also { v.duplicate().get(it) }
        else -> v
    }
}

private fun structToMap(s: Struct): MutableMap<String, Any?> =
    s.schema().fields().associateTo(mutableMapOf()) { f -> f.name() to convertValue(s.get(f), f.schema()) }
