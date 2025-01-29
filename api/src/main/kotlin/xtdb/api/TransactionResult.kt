package xtdb.api

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.*
import kotlinx.serialization.json.*
import xtdb.AnySerde
import xtdb.InstantSerde
import xtdb.util.requiringResolve
import java.time.Instant

@Serializable(with = Serde::class)
sealed interface TransactionResult : TransactionKey

internal class Serde : JsonContentPolymorphicSerializer<TransactionResult>(TransactionResult::class) {
    override fun selectDeserializer(element: JsonElement): DeserializationStrategy<TransactionResult> =
        if (element.jsonObject["committed"]?.jsonPrimitive?.boolean ?: throw SerializationException("Missing committed")) {
            TransactionCommitted.serializer()
        } else {
            TransactionAborted.serializer()
        }
}

@Serializable(with = CommittedSerde::class)
interface TransactionCommitted : TransactionResult {
    companion object {
        operator fun invoke(txId: Long, systemTime: Instant) =
            requiringResolve("xtdb.serde/->tx-committed").invoke(txId, systemTime) as TransactionCommitted
    }
}

internal class CommittedSerde : KSerializer<TransactionCommitted> {
    override val descriptor = buildClassSerialDescriptor("xtdb.api.TransactionCommitted") {
        element<Long>("txId")
        element("systemTime", InstantSerde.descriptor)
        element<Boolean>("committed")
    }

    override fun deserialize(decoder: Decoder): TransactionCommitted {
        var txId: Long? = null
        var systemTime: Instant? = null
        decoder.decodeStructure(descriptor) {
            loop@ while (true) {
                when (val index = decodeElementIndex(descriptor)) {
                    CompositeDecoder.DECODE_DONE -> break@loop
                    0 -> txId = decodeLongElement(descriptor, 0)
                    1 -> systemTime = decodeSerializableElement(descriptor, 1, InstantSerde)
                    2 -> Unit
                    else -> throw SerializationException("Unknown index $index")
                }
            }
        }

        return TransactionCommitted(
            txId ?: throw SerializationException("Missing txId"),
            systemTime ?: throw SerializationException("Missing systemTime")
        )
    }

    override fun serialize(encoder: Encoder, value: TransactionCommitted) =
        encoder.encodeStructure(descriptor) {
            encodeLongElement(descriptor, 0, value.txId)
            encodeSerializableElement(descriptor, 1, InstantSerde, value.systemTime)
            encodeBooleanElement(descriptor, 2, true)
        }
}

@Serializable(with = AbortedSerde::class)
interface TransactionAborted : TransactionResult {
    val error: Throwable

    companion object {
        operator fun invoke(txId: Long, systemTime: Instant, error: Throwable) =
            requiringResolve("xtdb.serde/->tx-aborted").invoke(txId, systemTime, error) as TransactionAborted
    }
}

internal class AbortedSerde : KSerializer<TransactionAborted> {
    override val descriptor = buildClassSerialDescriptor("xtdb.api.TransactionAborted") {
        element<Long>("txId")
        element("systemTime", InstantSerde.descriptor)
        element<Boolean>("committed")
        element("error", AnySerde.descriptor)
    }

    override fun deserialize(decoder: Decoder): TransactionAborted {
        var txId: Long? = null
        var systemTime: Instant? = null
        var error: Throwable? = null

        decoder.decodeStructure(descriptor) {
            loop@ while (true) {
                when (val index = decodeElementIndex(descriptor)) {
                    CompositeDecoder.DECODE_DONE -> break@loop
                    0 -> txId = decodeLongElement(descriptor, 0)
                    1 -> systemTime = decodeSerializableElement(descriptor, 1, InstantSerde)
                    2 -> Unit
                    3 -> error = decodeSerializableElement(descriptor, 3, AnySerde) as? Throwable
                    else -> throw SerializationException("Unknown index $index")
                }
            }
        }
        return TransactionAborted(
            txId ?: throw SerializationException("Missing txId"),
            systemTime ?: throw SerializationException("Missing systemTime"),
            error ?: throw SerializationException("Missing error")
        )
    }

    override fun serialize(encoder: Encoder, value: TransactionAborted) =
        encoder.encodeStructure(descriptor) {
            encodeLongElement(descriptor, 0, value.txId)
            encodeSerializableElement(descriptor, 1, InstantSerde, value.systemTime)
            encodeBooleanElement(descriptor, 2, false)
            encodeSerializableElement(descriptor, 3, AnySerde, value.error)
        }
}