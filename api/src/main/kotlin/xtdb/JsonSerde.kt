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
import xtdb.error.Incorrect
import java.io.InputStream
import java.io.OutputStream
import java.math.BigDecimal
import java.time.*
import java.time.temporal.Temporal
import java.util.*
import xtdb.table.TableRef
import xtdb.time.Interval

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
        is Temporal -> JsonPrimitive(toString())
        is ZoneId -> JsonPrimitive(toString())
        is Period -> JsonPrimitive(toString())
        is Date -> toInstant().toJsonElement()
        is Duration -> JsonPrimitive(toString())
        is Interval -> JsonPrimitive(toString())
        is TableRef -> toJsonLdElement()
        is Throwable -> toJsonLdElement()
        else -> throw Incorrect("unknown type: ${this.javaClass.name}")
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
