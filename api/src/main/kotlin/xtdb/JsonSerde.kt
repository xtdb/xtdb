@file:JvmName("JsonSerde")

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
import xtdb.util.kebabToCamelCase
import java.io.InputStream
import java.io.OutputStream
import java.math.BigDecimal
import java.time.*
import java.util.*

/**
 * @suppress
 */
object AnySerde : KSerializer<Any> {
    @OptIn(ExperimentalSerializationApi::class)
    override val descriptor = SerialDescriptor("xtdb.any", JsonElement.serializer().descriptor)

    private fun JsonElement.asString() = (this as? JsonPrimitive)?.takeIf { it.isString }?.content
    private fun JsonElement.asStringOrThrow() = asString() ?: throw jsonIAEwithMessage("@value must be string!", this)
    private fun JsonElement.asLong() = (this as? JsonPrimitive)?.longOrNull
    private fun JsonElement.asDouble() = (this as? JsonPrimitive)?.doubleOrNull

    private fun toThrowable(obj: JsonObject): Throwable {
        val errorMessage = obj["xtdb.error/message"]!!.asString()!!

        val dataObj = obj["xtdb.error/data"] as? JsonObject

        @Suppress("UNCHECKED_CAST")
        val errorData = PersistentHashMap.create(
            dataObj?.toValue()?.let { it as Map<String, *> }
                ?.mapKeys { (k, _) -> Keyword.intern(k) }
        )

        fun IPersistentMap.withCategory(cat: Keyword) = assoc(CATEGORY, cat)

        return when (obj["xtdb.error/class"]!!.asString()!!) {
            "xtdb.error.Incorrect" -> Incorrect(errorMessage, errorData.withCategory(INCORRECT))
            "xtdb.error.Unsupported" -> Unsupported(errorMessage, errorData.withCategory(UNSUPPORTED))
            "xtdb.error.Conflict" -> Conflict(errorMessage, errorData.withCategory(CONFLICT))
            "xtdb.error.Fault" -> Fault(errorMessage, errorData.withCategory(FAULT))
            "xtdb.error.Interrupted" -> Interrupted(errorMessage, errorData.withCategory(INTERRUPTED))
            "xtdb.error.NotFound" -> NotFound(errorMessage, errorData.withCategory(NOT_FOUND))
            "xtdb.error.Forbidden" -> Forbidden(errorMessage, errorData.withCategory(FORBIDDEN))
            "xtdb.error.Busy" -> Busy(errorMessage, errorData.withCategory(BUSY))
            "xtdb.error.Unavailable" -> Unavailable(errorMessage, errorData.withCategory(UNAVAILABLE))
            else -> ExceptionInfo(errorMessage, PersistentHashMap.create(errorData))
        }
    }

    fun JsonElement.toValue(): Any? = when (this) {
        is JsonArray -> map { it.toValue() }
        is JsonObject -> {
            val type = (this["@type"] as? JsonPrimitive)?.takeIf { it.isString }?.content

            if (type == null) {
                mapValues { (_, v) -> v.toValue() }.toMap()
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
                    "xt:duration" -> Duration.parse(value.asStringOrThrow())
                    "xt:timeZone" -> ZoneId.of(value.asStringOrThrow())
                    "xt:period" -> Period.parse(value.asStringOrThrow())
                    "xt:keyword" -> Keyword.intern(value.asStringOrThrow())
                    "xt:symbol" -> Symbol.intern(value.asStringOrThrow())
                    "xt:uuid" -> UUID.fromString(value.asStringOrThrow())
                    "xt:set" -> (value as? JsonArray ?: throw jsonIAEwithMessage(
                        "@value must be array!",
                        this
                    )).map { it.toValue() }.toSet()

                    "xt:error" -> toThrowable(
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

    private fun Any?.toJsonLdElement(type: String) = mapOf("@type" to type, "@value" to toString()).toJsonElement()

    private fun Throwable.toJsonLdElement() = mapOf(
        "@type" to "xt:error",
        "@value" to listOfNotNull(
            "xtdb.error/message" to message,
            "xtdb.error/class" to javaClass.name,
            (this as? IExceptionInfo)?.let {
                "xtdb.error/data" to (data as Map<*, *>).mapKeys { (k, _) -> (k as? Keyword)?.sym?.toString() ?: k }
                    .minus("cognitect.anomalies/category")
            }
        ).toMap()
    ).toJsonElement()

    private fun Any?.toJsonElement(): JsonElement = when (this) {
        null -> JsonNull
        is String -> JsonPrimitive(this)
        is BigDecimal -> toJsonLdElement("xt:decimal")
        is Number -> JsonPrimitive(this)
        is Boolean -> JsonPrimitive(this)
        is Map<*, *> -> JsonObject(map { (k, v) ->
            when (k) {
                is Keyword -> k.sym.toString() to v.toJsonElement()
                else -> k.toString() to v.toJsonElement()
            }
        }.toMap())

        is Set<*> -> mapOf("@type" to "xt:set", "@value" to toList()).toJsonElement()
        is Collection<*> -> JsonArray(map { it.toJsonElement() })
        is Keyword -> sym.toJsonLdElement("xt:keyword")
        is Symbol -> toJsonLdElement("xt:symbol")
        is UUID -> toJsonLdElement("xt:uuid")
        is ZonedDateTime -> toJsonLdElement("xt:timestamptz")
        is Instant -> toJsonLdElement("xt:instant")
        is LocalDate -> toJsonLdElement("xt:date")
        is LocalDateTime -> toJsonLdElement("xt:timestamp")
        is ZoneId -> toJsonLdElement("xt:timeZone")
        is Period -> toJsonLdElement("xt:period")
        is Date -> toInstant().toJsonElement()
        is Duration -> toJsonLdElement("xt:duration")
        is Throwable -> toJsonLdElement()
        else -> throw Incorrect("unknown type: ${this.javaClass.name}")
    }

    override fun deserialize(decoder: Decoder) =
        // top-level will be not-null, nested values may not be
        decoder.decodeSerializableValue(JsonElement.serializer()).toValue()!!

    override fun serialize(encoder: Encoder, value: Any) =
        encoder.encodeSerializableValue(JsonElement.serializer(), value.toJsonElement())
}

/**
 * @suppress
 */
@JvmField
val JSON_SERDE = Json {
    serializersModule =
        SerializersModule {
            contextual(AnySerde)
        }
}

/**
 * @suppress
 */
@JvmField
val JSON_SERDE_PRETTY_PRINT = Json(JSON_SERDE) { prettyPrint = true }

fun jsonIAE(errorType: String, element: JsonElement) =
    Incorrect(
        "Errant JSON",
        errorCode = errorType,
        data = mapOf("json" to JSON_SERDE_PRETTY_PRINT.encodeToString(element)),
    )

internal fun jsonIAEwithMessage(message: String, element: JsonElement) =
    Incorrect(message, data = mapOf("json" to JSON_SERDE_PRETTY_PRINT.encodeToString(element)))

private fun decodeError(value: String, cause: Throwable) =
    Incorrect("Error decoding JSON", "xtdb/json-decode-error", mapOf("json" to value), cause)

/**
 * @suppress
 */
fun decode(value: String): Any {
    try {
        return JSON_SERDE.decodeFromString(value)
    } catch (e: SerializationException) {
        throw decodeError(value, e)
    }
}

/**
 * @suppress
 */
@Suppress("unused")
@OptIn(InternalSerializationApi::class)
fun <T : Any> decode(value: String, clazz: Class<T>): Any {
    try {
        return JSON_SERDE.decodeFromString(clazz.kotlin.serializer(), value)
    } catch (e: SerializationException) {
        throw decodeError(value, e)
    }
}

/**
 * @suppress
 */
@Suppress("unused")
@OptIn(ExperimentalSerializationApi::class)
fun decode(inputStream: InputStream): Any {
    try {
        return JSON_SERDE.decodeFromStream(inputStream)
    } catch (e: SerializationException) {
        inputStream.reset()
        throw decodeError(inputStream.bufferedReader().use { it.readText() }, e)
    }
}

/**
 * @suppress
 */
@Suppress("unused")
@OptIn(InternalSerializationApi::class, ExperimentalSerializationApi::class)
fun <T : Any> decode(inputStream: InputStream, clazz: Class<T>): Any {
    try {
        return JSON_SERDE.decodeFromStream(deserializer = clazz.kotlin.serializer(), stream = inputStream)
    } catch (e: SerializationException) {
        try {
            inputStream.reset()
        } catch (t: Throwable) {
            e.addSuppressed(t)
        }

        throw decodeError(inputStream.bufferedReader().use { it.readText() }, e)
    }
}

/**
 * @suppress
 */
fun encode(value: Any): String = JSON_SERDE.encodeToString(value)

/**
 * @suppress
 */
@Suppress("unused")
@OptIn(InternalSerializationApi::class)
fun <T : Any> encode(value: T, clazz: Class<T>) = JSON_SERDE.encodeToString(clazz.kotlin.serializer(), value)

/**
 * @suppress
 */
@Suppress("unused")
@OptIn(ExperimentalSerializationApi::class)
fun encode(value: Any, outputStream: OutputStream) {
    JSON_SERDE.encodeToStream(value, outputStream)
}

/**
 * @suppress
 */
@Suppress("unused")
@OptIn(InternalSerializationApi::class, ExperimentalSerializationApi::class)
fun <T : Any> encode(value: T, outputStream: OutputStream, clazz: Class<T>) {
    JSON_SERDE.encodeToStream(clazz.kotlin.serializer(), value, outputStream)
}

/**
 * @suppress
 */
@Suppress("unused")
fun encodeStatus(value: Map<String, Any?>): String {
    return JSON_SERDE.encodeToString(value.mapKeys { it.key.kebabToCamelCase() })
}

/**
 * @suppress
 */
@Suppress("unused")
fun encodePretty(value: Any) = JSON_SERDE_PRETTY_PRINT.encodeToString(value)

/**
 * @suppress
 */
@Suppress("unused")
@OptIn(InternalSerializationApi::class)
fun <T : Any> encodePretty(value: T, clazz: Class<T>) =
    JSON_SERDE_PRETTY_PRINT.encodeToString(clazz.kotlin.serializer(), value)
