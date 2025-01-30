@file:UseSerializers(InstantSerde::class)

package xtdb.api

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.*
import xtdb.InstantSerde
import xtdb.api.TransactionKey.Serde
import xtdb.util.requiringResolve
import java.time.Instant

/**
 * A key representing a single transaction on the log.
 */
@Serializable(with = Serde::class)
interface TransactionKey : Comparable<TransactionKey> {

    /**
     * @suppress
     */
    class Serde : KSerializer<TransactionKey> {

        override val descriptor: SerialDescriptor = buildClassSerialDescriptor("xtdb.api.TransactionKey") {
            element<Long>("txId")
            element("systemTime", InstantSerde.descriptor)
        }

        override fun deserialize(decoder: Decoder): TransactionKey {
            var txId: Long? = null
            var systemTime: Instant? = null
            decoder.decodeStructure(descriptor) {
                loop@ while (true) {
                    when (val index = decodeElementIndex(descriptor)) {
                        CompositeDecoder.DECODE_DONE -> break@loop
                        0 -> txId = decodeLongElement(descriptor, 0)
                        1 -> systemTime = decodeSerializableElement(descriptor, 1, InstantSerde)
                        else -> throw SerializationException("Unknown index $index")
                    }
                }
            }
            return TransactionKey(
                txId ?: throw SerializationException("Missing txId"),
                systemTime ?: throw SerializationException("Missing systemTime")
            )
        }

        override fun serialize(encoder: Encoder, value: TransactionKey) =
            encoder.encodeStructure(descriptor) {
                encodeLongElement(descriptor, 0, value.txId)
                encodeSerializableElement(descriptor, 1, InstantSerde, value.systemTime)
            }
    }

    /**
     * the transaction id - a monotonically increasing number.
     */
    val txId: Long

    /**
     * the time as recorded by the log.
     */
    val systemTime: Instant

    /**
     * @suppress
     */
    override fun compareTo(other: TransactionKey) = txId.compareTo(other.txId)

    companion object {
        operator fun invoke(txId: Long, systemTime: Instant) =
            requiringResolve("xtdb.serde/->TxKey")(txId, systemTime) as TransactionKey
    }
}
