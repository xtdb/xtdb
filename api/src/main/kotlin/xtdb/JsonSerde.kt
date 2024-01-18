@file:JvmName("JsonSerde")

package xtdb

import clojure.lang.*
import kotlinx.serialization.*
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import xtdb.api.TransactionKey
import xtdb.util.kebabToCamelCase
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import java.nio.file.Paths
import java.time.*
import java.util.*

object AnySerde : KSerializer<Any> {
    @OptIn(ExperimentalSerializationApi::class)
    override val descriptor = SerialDescriptor("xtdb.any", JsonElement.serializer().descriptor)

    private fun JsonElement.asString() = (this as? JsonPrimitive)?.takeIf { it.isString }?.content
    private fun JsonElement.asStringOrThrow() = asString() ?: throw jsonIAEwithMessage("@value must be string!", this)

    private fun toThrowable(obj: JsonObject): Throwable {
        val errorMessage = obj["xtdb.error/message"]!!.asString()!!

        val dataObj = obj["xtdb.error/data"] as? JsonObject

        @Suppress("UNCHECKED_CAST")
        val errorData = dataObj?.let { data ->
            (data.toValue() as Map<String, *>)
                .mapKeys { (k, _) -> Keyword.intern(k) }
        } ?: emptyMap()

        val errorKey = (obj["xtdb.error/error-key"]?.toValue() as? String)?.let { Keyword.intern(it) }

        return when (obj["xtdb.error/class"]!!.asString()!!) {
            "xtdb.IllegalArgumentException" -> IllegalArgumentException(errorKey, errorMessage, errorData)
            "xtdb.RuntimeException" -> RuntimeException(errorKey, errorMessage, errorData)
            else -> ExceptionInfo(errorMessage, PersistentHashMap.create(errorData))
        }
    }

    private fun JsonElement.toValue(): Any = when (this) {
        is JsonArray -> map { it.toValue() }
        is JsonObject -> {
            val type = (this["@type"] as? JsonPrimitive)?.takeIf { it.isString }?.content

            if (type == null) {
                mapValues { (_, v) -> v.toValue() }.toMap()
            } else {
                val value = this["@value"] ?: throw jsonIAEwithMessage("@value can't be null!", this)
                when (type) {
                    "xt:instant" -> Instant.parse(value.asStringOrThrow())
                    "xt:timestamptz" -> ZonedDateTime.parse(value.asStringOrThrow())
                    "xt:timestamp" -> LocalDateTime.parse(value.asStringOrThrow())
                    "xt:date" -> LocalDate.parse(value.asStringOrThrow())
                    "xt:duration" -> Duration.parse(value.asStringOrThrow())
                    "xt:timeZone" -> ZoneId.of(value.asStringOrThrow())
                    "xt:keyword" -> Keyword.intern(value.asStringOrThrow())
                    "xt:symbol" -> Symbol.intern(value.asStringOrThrow())
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

        is JsonPrimitive -> if (isString) content else booleanOrNull ?: longOrNull ?: doubleOrNull
        ?: throw jsonIAE("unknown-json-primitive", this)
    }

    private fun Any?.toJsonLdElement(type: String) = mapOf("@type" to type, "@value" to toString()).toJsonElement()

    private fun Throwable.toJsonLdElement() = mapOf(
        "@type" to "xt:error",
        "@value" to listOfNotNull(
            "xtdb.error/message" to message,
            "xtdb.error/class" to javaClass.name,
            (this as? IllegalArgumentException)?.let {
                "xtdb.error/error-key" to key?.sym?.toString()
            },
            (this as? RuntimeException)?.let {
                "xtdb.error/error-key" to key?.sym?.toString()
            },
            (this as? IExceptionInfo)?.let {
                "xtdb.error/data" to (data as Map<*, *>).mapKeys { (k, _) -> (k as? Keyword)?.sym?.toString() ?: k }
                    .minus("xtdb.error/error-key")
            }
        ).toMap()
    ).toJsonElement()

    private fun Any?.toJsonElement(): JsonElement = when (this) {
        null -> JsonNull
        is String -> JsonPrimitive(this)
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
        is ZonedDateTime -> toJsonLdElement("xt:timestamptz")
        is Instant -> toJsonLdElement("xt:instant")
        is LocalDate -> toJsonLdElement("xt:date")
        is LocalDateTime -> toJsonLdElement("xt:timestamp")
        is ZoneId -> toJsonLdElement("xt:timeZone")
        is Date -> toInstant().toJsonElement()
        is Duration -> toJsonLdElement("xt:duration")
        is Throwable -> toJsonLdElement()
        else -> throw IllegalArgumentException.createNoKey("unknown type: ${this.javaClass.name}", mapOf<String, Any>())
    }

    override fun deserialize(decoder: Decoder) =
        decoder.decodeSerializableValue(JsonElement.serializer()).toValue()

    override fun serialize(encoder: Encoder, value: Any) =
        encoder.encodeSerializableValue(JsonElement.serializer(), value.toJsonElement())
}

@JvmField
val JSON_SERDE = Json {
    serializersModule =
        SerializersModule {
            contextual(AnySerde)
        }
}

@JvmField
val JSON_SERDE_PRETTY_PRINT = Json(JSON_SERDE) { prettyPrint = true }

fun jsonIAE(errorType: String, element: JsonElement): IllegalArgumentException {
    return IllegalArgumentException(
        Keyword.intern(errorType),
        data = mapOf(Keyword.intern("json") to JSON_SERDE_PRETTY_PRINT.encodeToString(element))
    )
}

fun jsonIAEwithMessage(message: String, element: JsonElement): IllegalArgumentException {
    return IllegalArgumentException.createNoKey(
        message,
        mapOf(Keyword.intern("json") to JSON_SERDE_PRETTY_PRINT.encodeToString(element))
    )
}

fun decode(value: String): Any {
    try {
        return JSON_SERDE.decodeFromString(value)
    } catch (e: SerializationException) {
        throw IllegalArgumentException.createNoKey(
            "Error decoding JSON!",
            mapOf(Keyword.intern("json") to value),
            e
        )
    }
}

@Suppress("unused")
@OptIn(InternalSerializationApi::class)
fun <T : Any> decode(value: String, clazz: Class<T>): Any {
    try {
        return JSON_SERDE.decodeFromString(clazz.kotlin.serializer(), value)
    } catch (e: SerializationException) {
        throw IllegalArgumentException.createNoKey(
            "Error decoding JSON!",
            mapOf(Keyword.intern("json") to value),
            e
        )
    }
}

@Suppress("unused")
@OptIn(ExperimentalSerializationApi::class)
fun decode(inputStream: InputStream): Any {
    try {
        return JSON_SERDE.decodeFromStream(inputStream)
    } catch (e: SerializationException) {
        inputStream.reset()
        throw IllegalArgumentException.createNoKey(
            "Error decoding JSON!",
            mapOf(Keyword.intern("json") to inputStream.bufferedReader().use { it.readText() }),
            e
        )
    }
}

@Suppress("unused")
@OptIn(InternalSerializationApi::class, ExperimentalSerializationApi::class)
fun <T : Any> decode(inputStream: InputStream, clazz: Class<T>): Any {
    try {
        return JSON_SERDE.decodeFromStream(deserializer = clazz.kotlin.serializer(), stream = inputStream)
    } catch (e: SerializationException) {
        inputStream.reset()
        throw IllegalArgumentException.createNoKey(
            "Error decoding JSON!",
            mapOf(Keyword.intern("json") to inputStream.bufferedReader().use { it.readText() }),
            e
        )
    }
}

fun encode(value: Any): String = JSON_SERDE.encodeToString(value)

@Suppress("unused")
@OptIn(InternalSerializationApi::class)
fun <T : Any> encode(value: T, clazz: Class<T>) = JSON_SERDE.encodeToString(clazz.kotlin.serializer(), value)

@Suppress("unused")
@OptIn(ExperimentalSerializationApi::class)
fun encode(value: Any, outputStream: OutputStream) {
    JSON_SERDE.encodeToStream(value, outputStream)
}

@Suppress("unused")
@OptIn(InternalSerializationApi::class, ExperimentalSerializationApi::class)
fun <T : Any> encode(value: T, outputStream: OutputStream, clazz: Class<T>) {
    JSON_SERDE.encodeToStream(clazz.kotlin.serializer(), value, outputStream)
}

@Suppress("unused")
fun encodeStatus(value: Map<String, TransactionKey>): String {
    return JSON_SERDE.encodeToString(value.mapKeys { it.key.kebabToCamelCase()})
}
