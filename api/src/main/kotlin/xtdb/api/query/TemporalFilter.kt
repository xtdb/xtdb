package xtdb.api.query

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import xtdb.api.query.Exprs.`val`
import xtdb.error.Incorrect
import xtdb.jsonIAE
import java.time.Instant

internal object TemporalExtentsSerializer : KSerializer<TemporalFilter.TemporalExtents> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("xtdb.api.query.TemporalFilter")

    override fun serialize(encoder: Encoder, value: TemporalFilter.TemporalExtents) {
        require(encoder is JsonEncoder)
        when (value) {
            is TemporalFilter.AllTime -> encoder.encodeString("allTime")
            is TemporalFilter.In -> encoder.encodeJsonElement(
                when {
                    value.to != null && value.from != null -> {
                        buildJsonObject {
                            put("in", buildJsonArray {
                                add(encoder.json.encodeToJsonElement(value.from))
                                add(encoder.json.encodeToJsonElement(value.to))
                            })
                        }
                    }

                    value.to != null -> buildJsonObject { put("to", encoder.json.encodeToJsonElement(value.to)) }
                    value.from != null -> buildJsonObject { put("from", encoder.json.encodeToJsonElement(value.from)) }
                    else -> throw Incorrect(
                        "Invalid temporal filter",
                        errorCode = "xtql/invalid-temporal-filter",
                        data = mapOf("temporal-filter" to value.toString())
                    )
                })
        }
    }

    fun deserialize(decoder: Decoder, element: JsonElement): TemporalFilter.TemporalExtents {
        require(decoder is JsonDecoder)

        return when (element) {
            is JsonPrimitive -> when {
                element.isString -> element.contentOrNull?.let {
                    if (it == "allTime") TemporalFilter.AllTime
                    else throw jsonIAE("xtql/malformed-temporal-filter", element)
                } ?: throw jsonIAE("xtql/malformed-temporal-filter", element)

                else -> throw jsonIAE("xtql/malformed-temporal-filter", element)
            }

            is JsonObject -> when {
                "from" in element -> TemporalFilters.from(
                    decoder.json.decodeFromJsonElement<Expr>(
                        element["from"] ?: throw jsonIAE("xtql/malformed-temporal-filter", element)
                    )
                )

                "to" in element -> TemporalFilters.to(
                    decoder.json.decodeFromJsonElement<Expr>(
                        element["to"] ?: throw jsonIAE("xtql/malformed-temporal-filter", element)
                    )
                )

                "in" in element -> {
                    val inElement = element["in"]
                    if (inElement !is JsonArray || inElement.size != 2) throw jsonIAE(
                        "xtql/malformed-temporal-filter",
                        element
                    )
                    TemporalFilters.`in`(
                        decoder.json.decodeFromJsonElement<Expr>(inElement[0]),
                        decoder.json.decodeFromJsonElement<Expr>(inElement[1])
                    )
                }

                else -> throw jsonIAE("xtql/malformed-temporal-filter", element)
            }

            else -> throw jsonIAE("xtql/malformed-temporal-filter", element)
        }

    }

    override fun deserialize(decoder: Decoder): TemporalFilter.TemporalExtents {
        require(decoder is JsonDecoder)
        return deserialize(decoder, decoder.decodeJsonElement())
    }
}

internal object TemporalFilterSerializer : KSerializer<TemporalFilter> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("xtdb.api.query.TemporalFilter")

    override fun serialize(encoder: Encoder, value: TemporalFilter) {
        require(encoder is JsonEncoder)
        when (value) {
            is TemporalFilter.At -> encoder.encodeJsonElement(
                buildJsonObject {
                    put("at", encoder.json.encodeToJsonElement(value.at))
                })

            else -> TemporalExtentsSerializer.serialize(encoder, value as TemporalFilter.TemporalExtents)
        }
    }

    override fun deserialize(decoder: Decoder): TemporalFilter {
        require(decoder is JsonDecoder)

        return when (val element = decoder.decodeJsonElement()) {
            is JsonObject -> when {
                "at" in element -> TemporalFilters.at(
                    decoder.json.decodeFromJsonElement<Expr>(
                        element["at"] ?: throw jsonIAE("xtql/malformed-temporal-filter", element)
                    )
                )

                else -> TemporalExtentsSerializer.deserialize(decoder, element)
            }

            else -> TemporalExtentsSerializer.deserialize(decoder, element)
        }
    }
}

@Serializable(TemporalFilterSerializer::class)
sealed interface TemporalFilter {

    @Serializable(TemporalExtentsSerializer::class)
    sealed interface TemporalExtents : TemporalFilter {
        val from: Expr?
        val to: Expr?
    }

    @Serializable
    object AllTime : TemporalFilter, TemporalExtents {
        override val from = null
        override val to = null
    }

    @Serializable
    data class At(val at: Expr) : TemporalFilter

    @Serializable
    data class In(override val from: Expr?, override val to: Expr?) : TemporalFilter, TemporalExtents
}

object TemporalFilters {

    @JvmField
    val allTime = TemporalFilter.AllTime

    @JvmStatic
    fun at(atExpr: Expr) = TemporalFilter.At(atExpr)

    @JvmStatic
    fun at(at: Instant) = at(`val`(at))

    @JvmStatic
    fun `in`(fromExpr: Expr?, toExpr: Expr?) = TemporalFilter.In(fromExpr, toExpr)

    @JvmStatic
    fun `in`(from: Instant?, to: Instant?) = `in`(from?.let(::`val`), to?.let(::`val`))

    @JvmStatic
    fun from(fromExpr: Expr) = TemporalFilter.In(fromExpr, null)

    @JvmStatic
    fun from(from: Instant) = from(`val`(from))

    @JvmStatic
    fun to(toExpr: Expr?) = TemporalFilter.In(null, toExpr)

    @JvmStatic
    fun to(to: Instant) = to(`val`(to))
}
