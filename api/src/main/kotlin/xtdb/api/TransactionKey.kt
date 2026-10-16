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
            element("ingestTime", InstantSerde.descriptor)
            element("systemTime", InstantSerde.descriptor)
        }

        override fun deserialize(decoder: Decoder): TransactionKey {
            var txId: Long? = null
            var ingestTime: Instant? = null
            var systemTime: Instant? = null
            decoder.decodeStructure(descriptor) {
                loop@ while (true) {
                    when (val index = decodeElementIndex(descriptor)) {
                        CompositeDecoder.DECODE_DONE -> break@loop
                        0 -> txId = decodeLongElement(descriptor, 0)
                        1 -> ingestTime = decodeSerializableElement(descriptor, 1, InstantSerde)
                        2 -> systemTime = decodeSerializableElement(descriptor, 2, InstantSerde)
                        else -> throw SerializationException("Unknown index $index")
                    }
                }
            }
            return TransactionKey(
                txId ?: throw SerializationException("Missing txId"),
                ingestTime ?: systemTime ?: throw SerializationException("Missing ingestTime"),
                systemTime,
            )
        }

        override fun serialize(encoder: Encoder, value: TransactionKey) =
            encoder.encodeStructure(descriptor) {
                encodeLongElement(descriptor, 0, value.txId)
                encodeSerializableElement(descriptor, 1, InstantSerde, value.ingestTime)
                value.systemTime?.let { encodeSerializableElement(descriptor, 2, InstantSerde, it) }
            }
    }

    /**
     * the transaction id - a monotonically increasing number.
     */
    val txId: Long

    /**
     * when the transaction was ingested - strictly increasing across a database's transactions.
     * DDL effects, and the transaction's own `xt.txs` row, take effect at this time.
     */
    val ingestTime: Instant

    /**
     * the system time the transaction's data writes take effect at, or null if it wrote no data rows -
     * DDL-only, aborted and empty transactions, for example.
     * An explicit `SYSTEM_TIME` may put it behind [ingestTime].
     */
    val systemTime: Instant?

    /**
     * @suppress
     */
    override fun compareTo(other: TransactionKey) = txId.compareTo(other.txId)

    companion object {
        operator fun invoke(txId: Long, ingestTime: Instant, systemTime: Instant?) =
            requiringResolve("xtdb.serde/->TxKey")(txId, ingestTime, systemTime) as TransactionKey
    }
}

typealias TxId = Long
