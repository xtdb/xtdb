@file:UseSerializers(AnySerde::class, InstantSerde::class, DurationSerde::class, ZoneIdSerde::class)

package xtdb.http

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import xtdb.*
import xtdb.AnySerde.toValue
import xtdb.api.TransactionKey
import xtdb.api.query.IKeyFn
import java.time.Duration
import java.time.ZoneId

@Serializable
data class QueryOptions(
    @Serializable(ArgsSerde::class) val args: Map<String, *>? = null,
    val basis: Basis? = null,
    val afterTx: TransactionKey? = null,
    val txTimeout: Duration? = null,
    val defaultTz: ZoneId? = null,
    val explain: Boolean = false,
    val keyFn: IKeyFn<*>? = null
) {

    internal object ArgsSerde: KSerializer<Map<String, *>> {
        @OptIn(ExperimentalSerializationApi::class)
        override val descriptor = SerialDescriptor("xtdb.ArgsSerde", JsonElement.serializer().descriptor)

        override fun serialize(encoder: Encoder, value: Map<String, *>) = AnySerde.serialize(encoder, value)

        @Suppress("UNCHECKED_CAST")
        override fun deserialize(decoder: Decoder): Map<String, *> {
            require(decoder is JsonDecoder)
            return when (val element = decoder.decodeJsonElement()) {
                is JsonArray -> element.map { it.toValue() } .mapIndexed{ idx, arg -> "_$idx" to arg }.toMap()
                is JsonObject -> element.toValue()
                else -> throw jsonIAE("unknown-args-json-type", element)
            } as Map<String, *>
        }
    }
}