package xtdb.kafkaconnectsource

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
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.indexer.OpenTx
import xtdb.indexer.TxIndexer
import xtdb.indexer.TxIndexer.TxResult
import xtdb.kafkaconnectsource.proto.DocsIndexerConfig
import xtdb.kafkaconnectsource.proto.docsIndexerConfig
import xtdb.kafkaconnectsource.proto.kafkaConnectSourceToken
import xtdb.table.TableRef
import xtdb.util.asIid
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset
import com.google.protobuf.Any as ProtoAny

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

/**
 * One [SinkRecord] = one XT transaction. The value [Struct] (or [Map]/primitive for schemaless converters)
 * is walked against its [Schema] and written as a doc. Tombstones (null value) become deletes.
 *
 * Connect's logical types map through to native XT types:
 *   Decimal      → BigDecimal
 *   Timestamp    → Instant
 *   Date         → LocalDate (UTC)
 *   Time         → LocalTime (UTC)
 */
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

        override fun toProto(): ProtoAny =
            ProtoAny.pack(docsIndexerConfig { table = this@Factory.table }, PROTO_TAG_PREFIX)

        class Registration : RecordIndexer.Registration {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.kafkaconnectsource.proto.DocsIndexerConfig"

            override fun fromProto(msg: ProtoAny): RecordIndexer.Factory {
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
                    val coords = coordsFor(rec)
                    TxResult.Aborted(wrapWithCoords(e, rec, coords), userMetadata = coords)
                }
            }
        }
    }

    private fun coordsFor(rec: SinkRecord): Map<String, Any?> =
        mapOf("topic" to rec.topic(), "partition" to rec.kafkaPartition(), "offset" to rec.kafkaOffset())

    private fun wrapWithCoords(cause: Throwable, rec: SinkRecord, coords: Map<String, Any?>): Throwable =
        Fault(
            "!Docs indexer failed at ${rec.topic()}-${rec.kafkaPartition()} offset ${rec.kafkaOffset()}: ${cause.message}",
            "xtdb.kafka-connect-source/docs-indexer-failed",
            coords,
            cause = cause,
        )

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

// Connect's StringConverter produces String keys; AvroConverter et al may yield a Struct
// with one field — in that case unwrap. ByteArrayConverter passes bytes through and asIid
// handles those directly.
private fun unwrapKeyForId(key: Any): Any =
    if (key is Struct && key.schema()?.fields()?.size == 1)
        key.get(key.schema().fields()[0])
    else key


// -- Value walk ------------------------------------------------------------------------------------
//
// Schema-first dispatch, matching Kafka Connect's own pattern (cf. JsonConverter.convertToJson):
//   1. null
//   2. logical-type check via schema.name() (overrides the primitive interpretation)
//   3. schema.type() switch
//   4. schemaless fallback dispatching on the JVM class
//
// Connect's Date/Time logical types are a `java.util.Date` pinned to 1970-01-01 UTC (see
// Time.toLogical / Date.toLogical in connect-api). The explicit `ZoneOffset.UTC` below is
// load-bearing — without it `toLocalDate()` / `toLocalTime()` use the JVM default zone and can
// shift the answer across a date or hour boundary.

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

    // Schemaless: dispatch on JVM class as a fallback.
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
