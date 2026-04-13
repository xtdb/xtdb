package xtdb.debezium

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import xtdb.error.Incorrect
import java.nio.ByteBuffer

private fun avroToJvmValue(value: Any?): Any? = when (value) {
    null -> null
    is GenericRecord -> value.schema.fields.associate { field ->
        field.name() to avroToJvmValue(value.get(field.name()))
    }
    is GenericEnumSymbol<*> -> value.toString()
    is GenericFixed -> value.bytes().copyOf()
    is GenericArray<*> -> value.map { avroToJvmValue(it) }
    is ByteBuffer -> ByteArray(value.remaining()).also { value.duplicate().get(it) }
    is Utf8 -> value.toString()
    is CharSequence -> value.toString()
    is Map<*, *> -> value.entries.associate { (k, v) -> k.toString() to avroToJvmValue(v) }
    else -> value // primitives: Int, Long, Float, Double, Boolean
}

/**
 * Decodes raw Kafka consumer values into CDC payload maps.
 *
 * Json expects ByteArray (from ByteArrayDeserializer).
 * Avro expects GenericRecord (from KafkaAvroDeserializer).
 */
@Serializable
sealed interface MessageFormat {
    fun decode(value: Any): Map<String, Any?>

    @Serializable
    @SerialName("!Json")
    data object Json : MessageFormat {
        override fun decode(value: Any): Map<String, Any?> {
            val bytes = value as? ByteArray
                ?: throw Incorrect("Expected ByteArray for JSON message format, got ${value::class.simpleName}")
            return parseCdcEnvelope(String(bytes, Charsets.UTF_8))
        }
    }

    @Serializable
    @SerialName("!Avro")
    data object Avro : MessageFormat {
        override fun decode(value: Any): Map<String, Any?> {
            val record = value as? GenericRecord
                ?: throw Incorrect("Expected GenericRecord for Avro message format, got ${value::class.simpleName}")

            return record.schema.fields.associate { field ->
                field.name() to avroToJvmValue(record.get(field.name()))
            }
        }
    }
}
