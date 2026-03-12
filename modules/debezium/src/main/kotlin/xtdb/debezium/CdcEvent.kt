package xtdb.debezium

import kotlinx.serialization.json.*
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.Relation
import xtdb.arrow.Vector
import xtdb.error.Incorrect
import xtdb.tx.TxOp
import java.time.DateTimeException
import java.time.Instant

internal fun JsonElement.toJvmValue(): Any? = when (this) {
    is JsonNull -> null
    is JsonPrimitive -> if (isString) content else booleanOrNull ?: longOrNull ?: doubleOrNull ?: content
    is JsonArray -> map { it.toJvmValue() }
    is JsonObject -> toJvmMap()
}

internal fun JsonObject.toJvmMap(): Map<String, Any?> =
    entries.associate { (k, v) -> k to v.toJvmValue() }

sealed class CdcEvent {
    abstract val schema: String
    abstract val table: String
    abstract val lsn: Long?
    abstract val txId: Long?

    abstract fun toTxOp(allocator: BufferAllocator): TxOp

    val metadata: Map<String, Any?>
        get() = buildMap {
            put("source", "debezium")
            lsn?.let { put("lsn", it) }
            txId?.let { put("tx_id", it) }
        }

    data class Put(
        override val schema: String,
        override val table: String,
        override val lsn: Long?,
        override val txId: Long?,
        val doc: Map<String, Any?>,
        val validFrom: Instant? = null,
        val validTo: Instant? = null,
    ) : CdcEvent() {
        override fun toTxOp(allocator: BufferAllocator): TxOp {
            val rel = Relation.openFromRows(allocator, listOf(doc))
            return TxOp.PutDocs(schema, table, validFrom, validTo, rel)
        }
    }

    data class Delete(
        override val schema: String,
        override val table: String,
        override val lsn: Long?,
        override val txId: Long?,
        val id: Any,
    ) : CdcEvent() {
        override fun toTxOp(allocator: BufferAllocator): TxOp {
            val ids = Vector.fromList(allocator, "_id", listOf(id))
            return TxOp.DeleteDocs(schema, table, null, null, ids)
        }
    }

    companion object {
        private fun parseValidTime(value: Any?, field: String): Instant? = when (value) {
            null -> null
            is String -> try {
                Instant.parse(value)
            } catch (e: DateTimeException) {
                throw Incorrect("Invalid ISO-8601 timestamp for '$field': $value")
            }
            else -> throw Incorrect("'$field' must be a TIMESTAMPTZ string, got ${value::class.simpleName}")
        }

        fun fromJson(bytes: ByteArray): CdcEvent {
            val envelope = try {
                Json.parseToJsonElement(String(bytes)).jsonObject
            } catch (e: IllegalArgumentException) {
                throw Incorrect("Invalid CDC message: ${e.message}", cause = e)
            }
            // Auto-detect JsonConverter envelope (schemas.enable=true wraps in {schema, payload}).
            // TODO: revisit when we add schema management — may want to use the schema field.
            val payload = envelope["payload"]?.jsonObject ?: envelope

            val op = payload["op"]?.jsonPrimitive?.content
                ?: throw Incorrect("Missing 'op' in payload")

            val source = payload["source"]?.jsonObject
                ?: throw Incorrect("Missing 'source' in payload")
            val schema = source["schema"]?.jsonPrimitive?.content
                ?: throw Incorrect("Missing 'source.schema' in payload")
            val table = source["table"]?.jsonPrimitive?.content
                ?: throw Incorrect("Missing 'source.table' in payload")
            val lsn = source["lsn"]?.jsonPrimitive?.longOrNull
            val txId = source["txId"]?.jsonPrimitive?.longOrNull

            return when (op) {
                "c", "r", "u" -> {
                    val after = payload["after"]?.jsonObject
                        ?: throw Incorrect("Missing 'after' for put op")

                    val docMap = after.toJvmMap().toMutableMap()

                    if ("_id" !in docMap) {
                        throw Incorrect("Missing '_id' in document")
                    }

                    // Debezium sends TIMESTAMPTZ as ISO-8601 strings
                    val validFrom = parseValidTime(docMap.remove("_valid_from"), "_valid_from")
                    val validTo = parseValidTime(docMap.remove("_valid_to"), "_valid_to")

                    if (validTo != null && validFrom == null) {
                        throw Incorrect("'_valid_to' requires '_valid_from'")
                    }

                    Put(schema, table, lsn, txId, docMap, validFrom, validTo)
                }

                "d" -> {
                    val before = payload["before"]?.takeUnless { it is JsonNull }?.jsonObject
                        ?: throw Incorrect("Missing 'before' for delete — check REPLICA IDENTITY on source table")

                    val id = before["_id"]?.toJvmValue()
                        ?: throw Incorrect("Missing '_id' in 'before' for delete")

                    Delete(schema, table, lsn, txId, id)
                }

                else -> throw Incorrect("Unknown CDC op: '$op'")
            }
        }
    }
}
