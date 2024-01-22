package xtdb.api.query

import clojure.lang.Keyword
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import xtdb.IllegalArgumentException
import xtdb.jsonIAE


object TemporalExtentsSerializer : KSerializer<TemporalFilter.TemporalExtents> {
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
                    else -> throw IllegalArgumentException.create(
                        Keyword.intern("xtql/invalid-temporal-filter"),
                        mapOf(Keyword.intern("temporal-filter") to value.toString())
                    )
                })
        }
    }

    fun deserialize(decoder: Decoder, element: JsonElement) : TemporalFilter.TemporalExtents {
        require(decoder is JsonDecoder)

        return when (element) {
            is JsonPrimitive -> when {
                element.isString -> element.contentOrNull?.let {
                    if (it == "allTime") TemporalFilter.ALL_TIME
                    else throw jsonIAE("xtql/malformed-temporal-filter", element)
                } ?: throw jsonIAE("xtql/malformed-temporal-filter", element)

                else -> throw jsonIAE("xtql/malformed-temporal-filter", element)
            }

            is JsonObject -> when {
                "from" in element -> TemporalFilter.from(
                    decoder.json.decodeFromJsonElement<Expr>(
                        element["from"] ?: throw jsonIAE("xtql/malformed-temporal-filter", element)
                    )
                )

                "to" in element -> TemporalFilter.to(
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
                    TemporalFilter.`in`(
                        decoder.json.decodeFromJsonElement(inElement[0]),
                        decoder.json.decodeFromJsonElement(inElement[1])
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


object TemporalFilterSerializer : KSerializer<TemporalFilter> {
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
                "at" in element -> TemporalFilter.at(
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

    companion object {
        @JvmStatic
        fun at(atExpr: Expr) = At(atExpr)

        @JvmStatic
        fun `in`(fromExpr: Expr?, toExpr: Expr?) = In(fromExpr, toExpr)

        @JvmStatic
        fun from(fromExpr: Expr?) = In(fromExpr, null)

        @JvmStatic
        fun to(toExpr: Expr?) = In(null, toExpr)

        @JvmField
        val ALL_TIME = AllTime
    }
}
