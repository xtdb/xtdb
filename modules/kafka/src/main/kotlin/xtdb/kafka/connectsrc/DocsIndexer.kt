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
import xtdb.error.Anomaly.Companion.toAnomaly
import xtdb.error.Incorrect
import xtdb.indexer.OpenTx
import xtdb.indexer.TxIndexer
import xtdb.indexer.TxIndexer.TxResult
import xtdb.kafka.connectsrc.proto.DocsIndexerConfig
import xtdb.kafka.connectsrc.proto.docsIndexerConfig
import xtdb.kafka.connectsrc.proto.kafkaConnectSourceToken
import xtdb.table.TableRef
import xtdb.util.asIid
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneOffset
import com.google.protobuf.Any as ProtoAny

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

class DocsIndexer(
    private val table: TableRef,
) : RecordIndexer {

    @Serializable
    @SerialName("!Docs")
    data class Factory(
        val table: String,
    ) : RecordIndexer.Factory {

        override fun open(dbName: String): RecordIndexer =
            DocsIndexer(TableRef.parse(dbName, table))

        class Registration : RecordIndexer.Registration<Factory> {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.kafka.connectsrc.proto.DocsIndexerConfig"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(docsIndexerConfig { table = factory.table }, PROTO_TAG_PREFIX)

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

            txIndexer.indexTx(token, systemTime = systemTime) { openTx ->
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

    private fun coordsFor(rec: SinkRecord): Map<String, Any?> =
        mapOf("topic" to rec.topic(), "partition" to rec.kafkaPartition(), "offset" to rec.kafkaOffset())

    private fun writeRecord(openTx: OpenTx, rec: SinkRecord) {
        val openTxTable = openTx.table(table)
        val value = rec.value()

        if (value == null) {
            val id = rec.key() ?: throw Incorrect(
                "tombstone has no key — can't resolve _id",
                "xtdb.kafka-connect-source/docs-no-id-on-tombstone",
                mapOf("topic" to rec.topic(), "partition" to rec.kafkaPartition(), "offset" to rec.kafkaOffset()),
            )
            openTxTable.logDelete(
                ByteBuffer.wrap(unwrapKeyForId(id).asIid),
                openTx.systemFrom,
                Long.MAX_VALUE,
            )
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

        val id = docMap["_id"] ?: rec.key()?.let { unwrapKeyForId(it) }?.also { docMap["_id"] = it }
            ?: throw Incorrect(
                "no _id on doc and no record key — can't resolve _id",
                "xtdb.kafka-connect-source/docs-no-id",
                mapOf("topic" to rec.topic(), "partition" to rec.kafkaPartition(), "offset" to rec.kafkaOffset()),
            )

        openTxTable.logPut(
            ByteBuffer.wrap(id.asIid),
            openTx.systemFrom,
            Long.MAX_VALUE,
        ) { openTxTable.docWriter.writeObject(docMap) }
    }
}

// AvroConverter et al may yield a single-field Struct as the key — unwrap to the inner value.
private fun unwrapKeyForId(key: Any): Any =
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
                .associateTo(mutableMapOf<String, Any?>()) { (k, vv) -> k.toString() to convertValue(vv, schema.valueSchema()) }
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
        is Map<*, *> -> v.entries.associateTo(mutableMapOf<String, Any?>()) { (k, vv) -> k.toString() to convertValue(vv, null) }
        is ByteBuffer -> ByteArray(v.remaining()).also { v.duplicate().get(it) }
        else -> v
    }
}

private fun structToMap(s: Struct): MutableMap<String, Any?> =
    s.schema().fields().associateTo(mutableMapOf()) { f -> f.name() to convertValue(s.get(f), f.schema()) }
