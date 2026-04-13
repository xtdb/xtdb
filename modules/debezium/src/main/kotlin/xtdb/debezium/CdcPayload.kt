package xtdb.debezium

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.longOrNull
import xtdb.error.Incorrect
import xtdb.indexer.OpenTx
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.util.asIid
import java.nio.ByteBuffer
import java.time.DateTimeException
import java.time.Instant

// JSON → plain JVM values.  Used by MessageFormat.Json (ByteArray input) and
// DebeziumEngineSource (String input) — kept here rather than duplicated.
internal fun parseCdcEnvelope(json: String): Map<String, Any?> {
    val envelope = try {
        Json.parseToJsonElement(json).jsonObject
    } catch (e: RuntimeException) {
        throw Incorrect("Invalid CDC message: ${e.message}", cause = e)
    }

    // Auto-detect Kafka Connect JsonConverter envelope (`{schema, payload}`).
    val payload = envelope["payload"]?.let {
        (it as? JsonObject) ?: throw Incorrect("'payload' was not a JSON object")
    } ?: envelope

    return payload.entries.associate { (k, v) -> k to jsonToJvmValue(v) }
}

internal fun jsonToJvmValue(el: Any?): Any? = when (el) {
    null, is JsonNull -> null
    is JsonPrimitive -> if (el.isString) el.content else el.booleanOrNull ?: el.longOrNull ?: el.doubleOrNull ?: el.content
    is JsonArray -> el.map { jsonToJvmValue(it) }
    is JsonObject -> el.entries.associate { (k, v) -> k to jsonToJvmValue(v) }
    else -> el
}

// Shared CDC payload handling for the Debezium sources (both Kafka and embedded engine).
// Both sources ultimately receive the same Debezium envelope shape —
// { op, before, after, source: { schema, table, lsn, txId, snapshot, ... } } — so the
// write path into XTDB is the same. Transport-specific code lives in each source.

internal fun parseValidTimeMicros(value: Any?, field: String): Long? = when (value) {
    null -> null
    is String -> try {
        Instant.parse(value).asMicros
    } catch (_: DateTimeException) {
        throw Incorrect("Invalid ISO-8601 timestamp for '$field': $value")
    }

    else -> throw Incorrect("'$field' must be a TIMESTAMPTZ string, got ${value::class.simpleName}")
}

@Suppress("UNCHECKED_CAST")
internal fun writeCdcPayload(payload: Map<String, Any?>, dbName: String, openTx: OpenTx) {
    val op = payload["op"] as? String
        ?: throw Incorrect("Missing 'op' in payload")

    val source = payload["source"] as? Map<String, Any?>
        ?: throw Incorrect("Missing 'source' in payload")
    val schema = source["schema"] as? String
        ?: throw Incorrect("Missing 'source.schema' in payload")
    val table = source["table"] as? String
        ?: throw Incorrect("Missing 'source.table' in payload")

    val openTxTable = openTx.table(TableRef(dbName, schema, table))

    when (op) {
        "c", "r", "u" -> {
            val after = payload["after"] as? Map<String, Any?>
                ?: throw Incorrect("Missing 'after' for put op")

            val docMap = after.toMutableMap()

            val id = docMap["_id"] ?: throw Incorrect("Missing '_id' in document")

            val explicitValidFrom = parseValidTimeMicros(docMap.remove("_valid_from"), "_valid_from")
            val explicitValidTo = parseValidTimeMicros(docMap.remove("_valid_to"), "_valid_to")

            if (explicitValidTo != null && explicitValidFrom == null)
                throw Incorrect("'_valid_to' requires '_valid_from'")

            openTxTable.logPut(
                ByteBuffer.wrap(id.asIid),
                explicitValidFrom ?: openTx.systemFrom,
                explicitValidTo ?: Long.MAX_VALUE,
            ) { openTxTable.docWriter.writeObject(docMap) }
        }

        "d" -> {
            val before = payload["before"]?.let { it as? Map<String, Any?> }
                ?: throw Incorrect("Missing 'before' for delete — check REPLICA IDENTITY on source table")

            val id = before["_id"]
                ?: throw Incorrect("Missing '_id' in 'before' for delete")

            openTxTable.logDelete(
                ByteBuffer.wrap(id.asIid),
                openTx.systemFrom,
                Long.MAX_VALUE,
            )
        }

        else -> throw Incorrect("Unknown CDC op: '$op'")
    }
}
