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
import xtdb.AnyJsonLdSerde.fromLdValue
import xtdb.AnyJsonLdSerde.toJsonLdElement
import xtdb.error.Anomaly.Companion.CATEGORY
import xtdb.error.Busy
import xtdb.error.Conflict
import xtdb.error.Fault
import xtdb.error.Forbidden
import xtdb.error.Incorrect
import xtdb.error.Interrupted
import xtdb.error.NotFound
import xtdb.error.Unavailable
import xtdb.error.Unsupported
import java.io.InputStream
import java.io.OutputStream
import java.math.BigDecimal
import java.time.*
import java.time.temporal.Temporal
import java.util.*
import xtdb.table.TableRef
import xtdb.time.Interval
import xtdb.util.requiringResolve

private val PR_STR by lazy { requiringResolve("clojure.core/pr-str") }
private val ERROR_CODE_KW = Keyword.intern("xtdb.error", "code")

/**
 * Simple JSON serializer - non-native types become strings
 * Uses JSON-LD to deserialize because JSON-LD is a superset of JSON
 *
 * @suppress
 */
object AnySerde : KSerializer<Any> {
    @OptIn(ExperimentalSerializationApi::class)
    override val descriptor = SerialDescriptor("xtdb.any", JsonElement.serializer().descriptor)

    private fun Any?.toJsonElement(): JsonElement = when (this) {
        null -> JsonNull
        is String -> JsonPrimitive(this)
        is BigDecimal -> JsonPrimitive(toString())
        is Number -> JsonPrimitive(this)
        is Boolean -> JsonPrimitive(this)
        is Map<*, *> -> JsonObject(map { (k, v) ->
            when (k) {
                is Keyword -> k.sym.toString() to v.toJsonElement()
                else -> k.toString() to v.toJsonElement()
            }
        }.toMap())
        is Set<*> -> JsonArray(map { it.toJsonElement() })
        is Collection<*> -> JsonArray(map { it.toJsonElement() })
        is Keyword -> JsonPrimitive(sym.toString())
        is Symbol -> JsonPrimitive(toString())
        is UUID -> JsonPrimitive(toString())
        is ZonedDateTime -> JsonPrimitive(toOffsetDateTime().toString())
        is Temporal -> JsonPrimitive(toString())
        is ZoneId -> JsonPrimitive(toString())
        is Period -> JsonPrimitive(toString())
        is Date -> toInstant().toJsonElement()
        is Duration -> JsonPrimitive(toString())
        is Interval -> JsonPrimitive(toString())
        is TableRef -> toJsonLdElement()
        is Throwable -> toSimpleJsonElement()
        else -> throw Incorrect("unknown type: ${this.javaClass.name}")
    }

    fun Throwable.toSimpleJsonElement(): JsonElement {
        val category = when (this) {
            is Incorrect -> "incorrect"
            is Unsupported -> "unsupported"
            is Conflict -> "conflict"
            is Fault -> "fault"
            is Interrupted -> "interrupted"
            is NotFound -> "not-found"
            is Forbidden -> "forbidden"
            is Busy -> "busy"
            is Unavailable -> "unavailable"
            else -> "error"
        }
        val rawData = (this as? IExceptionInfo)?.data as? Map<*, *> ?: emptyMap<Any, Any>()
        val code = (rawData[ERROR_CODE_KW] as? Keyword)?.sym?.toString()
        val displayData = rawData
            .filterKeys { it != CATEGORY && it != ERROR_CODE_KW }
            .mapKeys { (k, _) -> (k as? Keyword)?.sym?.toString() ?: k.toString() }

        return buildJsonObject {
            put("category", category)
            code?.let { put("code", it) }
            message?.let { put("message", it) }
            if (displayData.isNotEmpty()) {
                putJsonObject("data") {
                    for ((k, v) in displayData) {
                        put(k, v.toSimpleJsonValue())
                    }
                }
            }
        }
    }

    private fun Any?.toSimpleJsonValue(): JsonElement = when (this) {
        null -> JsonNull
        is String -> JsonPrimitive(this)
        is Number -> JsonPrimitive(this)
        is Boolean -> JsonPrimitive(this)
        is Keyword -> JsonPrimitive(sym.toString())
        is Symbol -> JsonPrimitive(toString())
        is Map<*, *> -> JsonObject(entries.associate { (k, v) ->
            val keyStr = (k as? Keyword)?.sym?.toString() ?: k?.toString() ?: "null"
            keyStr to v.toSimpleJsonValue()
        })
        is Set<*> -> JsonArray(map { it.toSimpleJsonValue() })
        is Collection<*> -> JsonArray(map { it.toSimpleJsonValue() })
        else -> JsonPrimitive(PR_STR.invoke(this) as String)
    }

    override fun deserialize(decoder: Decoder) =
        // top-level will be not-null, nested values may not be
        decoder.decodeSerializableValue(JsonElement.serializer()).fromLdValue()!!

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
fun encodeToBytes(value: Any): ByteArray = encode(value).toByteArray(Charsets.UTF_8)

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
