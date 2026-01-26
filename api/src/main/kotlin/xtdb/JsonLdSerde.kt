@file:JvmName("JsonLdSerde")

package xtdb

import clojure.lang.*
import kotlinx.serialization.*
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import xtdb.error.*
import xtdb.error.Anomaly.Companion.CATEGORY
import xtdb.error.Busy.Companion.BUSY
import xtdb.error.Conflict.Companion.CONFLICT
import xtdb.error.Fault.Companion.FAULT
import xtdb.error.Forbidden.Companion.FORBIDDEN
import xtdb.error.Incorrect.Companion.INCORRECT
import xtdb.error.Interrupted.Companion.INTERRUPTED
import xtdb.error.NotFound.Companion.NOT_FOUND
import xtdb.error.Unavailable.Companion.UNAVAILABLE
import xtdb.error.Unsupported.Companion.UNSUPPORTED
import java.io.InputStream
import java.io.OutputStream
import java.math.BigDecimal
import java.time.*
import java.util.*
import xtdb.table.TableRef
import xtdb.time.Interval
import xtdb.time.asInterval

/**
 * JSON-LD serializer with @type/@value wrappers for non-native JSON types
 * @suppress
 */
object AnyJsonLdSerde : KSerializer<Any> {
    @OptIn(ExperimentalSerializationApi::class)
    override val descriptor = SerialDescriptor("xtdb.any", JsonElement.serializer().descriptor)

    private fun JsonElement.asString() = (this as? JsonPrimitive)?.takeIf { it.isString }?.content
    private fun JsonElement.asStringOrThrow() = asString() ?: throw jsonIAEwithMessage("@value must be string!", this)
    private fun JsonElement.asLong() = (this as? JsonPrimitive)?.longOrNull
    private fun JsonElement.asDouble() = (this as? JsonPrimitive)?.doubleOrNull

    private fun toThrowable(type: String, obj: JsonObject): Throwable {
        val errorMessage = obj["xtdb.error/message"]?.asString()

        val dataObj = obj["xtdb.error/data"] as? JsonObject

        @Suppress("UNCHECKED_CAST")
        val errorData = PersistentHashMap.create(
            dataObj?.fromLdValue()?.let { it as Map<String, *> }
                ?.mapKeys { (k, _) -> Keyword.intern(k) }
        )

        fun IPersistentMap.withCategory(cat: Keyword) = assoc(CATEGORY, cat)

        return when (type) {
            "xt:incorrect" -> Incorrect(errorMessage, errorData.withCategory(INCORRECT))
            "xt:unsupported" -> Unsupported(errorMessage, errorData.withCategory(UNSUPPORTED))
            "xt:conflict" -> Conflict(errorMessage, errorData.withCategory(CONFLICT))
            "xt:fault" -> Fault(errorMessage, errorData.withCategory(FAULT))
            "xt:interrupted" -> Interrupted(errorMessage, errorData.withCategory(INTERRUPTED))
            "xt:not-found" -> NotFound(errorMessage, errorData.withCategory(NOT_FOUND))
            "xt:forbidden" -> Forbidden(errorMessage, errorData.withCategory(FORBIDDEN))
            "xt:busy" -> Busy(errorMessage, errorData.withCategory(BUSY))
            "xt:unavailable" -> Unavailable(errorMessage, errorData.withCategory(UNAVAILABLE))
            "xt:error" -> ExceptionInfo(errorMessage, PersistentHashMap.create(errorData))
            else -> throw jsonIAE("unknown-error-type", obj)
        }
    }

    private val ANOMALY_TYPES = setOf(
        "xt:incorrect", "xt:unsupported", "xt:conflict", "xt:fault", "xt:interrupted",
        "xt:not-found", "xt:forbidden", "xt:busy", "xt:unavailable", "xt:error"
    )

    fun JsonElement.fromLdValue(): Any? = when (this) {
        is JsonArray -> map { it.fromLdValue() }
        is JsonObject -> {
            val type = (this["@type"] as? JsonPrimitive)?.takeIf { it.isString }?.content

            if (type == null) {
                mapValues { (_, v) -> v.fromLdValue() }.toMap()
            } else {
                val value = this["@value"] ?: throw jsonIAEwithMessage("@value can't be null!", this)
                when (type) {
                    "xt:long" ->
                        value.asLong()
                            ?: value.asString()?.toLong()
                            ?: throw jsonIAEwithMessage("@value must be long!", this)

                    "xt:double" ->
                        value.asDouble()
                            ?: value.asString()?.toDouble()
                            ?: throw jsonIAEwithMessage("@value must be double!", this)

                    "xt:decimal" -> value.asStringOrThrow().toBigDecimal()

                    "xt:instant" -> Instant.parse(value.asStringOrThrow())
                    "xt:timestamptz" -> ZonedDateTime.parse(value.asStringOrThrow())
                    "xt:timestamp" -> LocalDateTime.parse(value.asStringOrThrow())
                    "xt:date" -> LocalDate.parse(value.asStringOrThrow())
                    "xt:time" -> LocalTime.parse(value.asStringOrThrow())
                    "xt:duration" -> Duration.parse(value.asStringOrThrow())
                    "xt:interval" -> value.asStringOrThrow().asInterval()
                    "xt:timeZone" -> ZoneId.of(value.asStringOrThrow())
                    "xt:period" -> Period.parse(value.asStringOrThrow())
                    "xt:keyword" -> Keyword.intern(value.asStringOrThrow())
                    "xt:symbol" -> Symbol.intern(value.asStringOrThrow())
                    "xt:uuid" -> UUID.fromString(value.asStringOrThrow())
                    "xt:set" -> (value as? JsonArray ?: throw jsonIAEwithMessage(
                        "@value must be array!",
                        this
                    )).map { it.fromLdValue() }.toSet()

                    "xt:table" -> {
                        val obj = value as? JsonObject ?: throw jsonIAEwithMessage("@value must be object!", this)
                        TableRef(
                            obj["db"]?.asStringOrThrow() ?: throw jsonIAEwithMessage("db is required!", this),
                            obj["schema"]?.asString() ?: "public",
                            obj["table"]?.asStringOrThrow() ?: throw jsonIAEwithMessage("table is required!", this)
                        )
                    }

                    in ANOMALY_TYPES -> toThrowable(
                        type,
                        value as? JsonObject ?: throw jsonIAEwithMessage(
                            "@value must be object!",
                            this
                        )
                    )

                    else -> throw jsonIAE("unknown-json-ld-type", this)
                }
            }
        }

        is JsonNull -> null

        is JsonPrimitive -> if (isString) content else booleanOrNull ?: longOrNull ?: doubleOrNull
        ?: throw jsonIAE("unknown-json-primitive", this)
    }

    fun Any?.toJsonLdElement(type: String) = mapOf("@type" to type, "@value" to toString()).toJsonLdElement()

    fun Throwable.toJsonLdElement(): JsonElement {
        val type = when (this) {
            is Incorrect -> "xt:incorrect"
            is Unsupported -> "xt:unsupported"
            is Conflict -> "xt:conflict"
            is Fault -> "xt:fault"
            is Interrupted -> "xt:interrupted"
            is NotFound -> "xt:not-found"
            is Forbidden -> "xt:forbidden"
            is Busy -> "xt:busy"
            is Unavailable -> "xt:unavailable"
            else -> "xt:error"
        }
        return mapOf(
            "@type" to type,
            "@value" to listOfNotNull(
                "xtdb.error/message" to message,
                (this as? IExceptionInfo)?.let {
                    "xtdb.error/data" to (data as Map<*, *>).mapKeys { (k, _) -> (k as? Keyword)?.sym?.toString() ?: k }
                        .minus("cognitect.anomalies/category")
                }
            ).toMap()
        ).toJsonLdElement()
    }

    fun Any?.toJsonLdElement(): JsonElement = when (this) {
        null -> JsonNull
        is String -> JsonPrimitive(this)
        is BigDecimal -> toJsonLdElement("xt:decimal")
        is Number -> JsonPrimitive(this)
        is Boolean -> JsonPrimitive(this)
        is Map<*, *> -> JsonObject(map { (k, v) ->
            when (k) {
                is Keyword -> k.sym.toString() to v.toJsonLdElement()
                else -> k.toString() to v.toJsonLdElement()
            }
        }.toMap())

        is Set<*> -> mapOf("@type" to "xt:set", "@value" to toList()).toJsonLdElement()
        is Collection<*> -> JsonArray(map { it.toJsonLdElement() })
        is Keyword -> sym.toJsonLdElement("xt:keyword")
        is Symbol -> toJsonLdElement("xt:symbol")
        is UUID -> toJsonLdElement("xt:uuid")
        is ZonedDateTime -> toJsonLdElement("xt:timestamptz")
        is Instant -> toJsonLdElement("xt:instant")
        is LocalDate -> toJsonLdElement("xt:date")
        is LocalTime -> toJsonLdElement("xt:time")
        is LocalDateTime -> toJsonLdElement("xt:timestamp")
        is ZoneId -> toJsonLdElement("xt:timeZone")
        is Period -> toJsonLdElement("xt:period")
        is Date -> toInstant().toJsonLdElement()
        is Duration -> toJsonLdElement("xt:duration")
        is Interval -> toJsonLdElement("xt:interval")
        is TableRef -> mapOf(
            "@type" to "xt:table",
            "@value" to mapOf("db" to dbName, "schema" to schemaName, "table" to tableName)
        ).toJsonLdElement()

        is Throwable -> toJsonLdElement()
        else -> throw Incorrect("unknown type: ${this.javaClass.name}")
    }

    override fun deserialize(decoder: Decoder) =
        // top-level will be not-null, nested values may not be
        decoder.decodeSerializableValue(JsonElement.serializer()).fromLdValue()!!

    override fun serialize(encoder: Encoder, value: Any) =
        encoder.encodeSerializableValue(JsonElement.serializer(), value.toJsonLdElement())
}

/**
 * @suppress
 */
@JvmField
val JSON_LD_SERDE = Json {
    serializersModule =
        SerializersModule {
            contextual(AnyJsonLdSerde)
        }
}

private fun decodeError(value: String, cause: Throwable) =
    Incorrect("Error decoding JSON-LD", "xtdb/json-ld-decode-error", mapOf("json" to value), cause)

/**
 * @suppress
 */
@Suppress("unused")
@OptIn(ExperimentalSerializationApi::class)
fun decodeJsonLd(json: String): Any =
    try {
        JSON_LD_SERDE.decodeFromString(AnyJsonLdSerde, json)
    } catch (e: Exception) {
        throw decodeError(json, e)
    }

/**
 * @suppress
 */
fun encodeJsonLd(value: Any): String = JSON_LD_SERDE.encodeToString(value)

/**
 * @suppress
 */
@Suppress("unused")
fun encodeJsonLdToBytes(value: Any): ByteArray = encodeJsonLd(value).toByteArray(Charsets.UTF_8)