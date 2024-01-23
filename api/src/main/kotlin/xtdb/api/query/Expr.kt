@file:UseSerializers(AnySerde::class)

package xtdb.api.query

import kotlinx.serialization.*
import kotlinx.serialization.descriptors.PrimitiveKind.*
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import xtdb.AnySerde
import xtdb.jsonIAE

private fun JsonElement.requireObject(errorType: String) = this as? JsonObject ?: throw jsonIAE(errorType, this)
private fun JsonElement.requireArray(errorType: String) = this as? JsonArray ?: throw jsonIAE(errorType, this)

private fun JsonObject.requireType(errorType: String) =
    apply { if ("@type" !in this) throw jsonIAE(errorType, this) }

private fun JsonObject.requireValue(errorType: String) = this["@value"] ?: throw jsonIAE(errorType, this)

@Serializable(Expr.Serde::class)
sealed interface Expr {
    object Serde : JsonContentPolymorphicSerializer<Expr>(Expr::class) {
        override fun selectDeserializer(element: JsonElement): DeserializationStrategy<Expr> = when (element) {
            JsonNull -> Null.serializer()
            is JsonPrimitive -> when {
                element.booleanOrNull != null -> Bool.serializer()
                element.longOrNull != null -> Long.serializer()
                element.doubleOrNull != null -> Double.serializer()
                element.isString -> Obj.serializer()
                else -> TODO("unknown primitive")
            }

            is JsonObject -> when {
                "xt:lvar" in element -> LogicVar.serializer()
                "xt:param" in element -> Param.serializer()
                "xt:call" in element -> Call.serializer()
                "xt:get " in element -> Get.serializer()
                "xt:q" in element -> Subquery.serializer()
                "xt:exists" in element -> Exists.serializer()
                "xt:pull" in element -> Pull.serializer()
                "xt:pullMany" in element -> PullMany.serializer()
                // TODO check against non string type Element
                "@type" in element -> {
                    val type = (element["@type"] as? JsonPrimitive)?.takeIf { it.isString }?.content
                    when (type) {
                        "xt:set" -> SetExpr.serializer()
                        else -> Obj.serializer()
                    }
                }

                else -> MapExpr.serializer()
            }

            is JsonArray -> ListExpr.serializer()
            else -> Obj.serializer()
        }
    }

    @Serializable(Null.Serde::class)
    data object Null : Expr {
        object Serde : KSerializer<Null> {
            @OptIn(ExperimentalSerializationApi::class)
            override val descriptor: SerialDescriptor = SerialDescriptor("xtdb.api.query.Expr.Null" ,JsonNull.serializer().descriptor)

            @OptIn(ExperimentalSerializationApi::class)
            override fun serialize(encoder: Encoder, value: Null) {
                require(encoder is JsonEncoder)
                encoder.encodeJsonElement(JsonNull)
            }

            override fun deserialize(decoder: Decoder) = Null
        }
    }

    @Serializable(Bool.Serde::class)
    enum class Bool(@JvmField val bool: Boolean) : Expr {
        TRUE(true), FALSE(false);

        object Serde : KSerializer<Bool> {
            override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("xtdb.api.query.Expr.Bool", BOOLEAN)
            override fun serialize(encoder: Encoder, value: Bool) {
                require(encoder is JsonEncoder)
                if (value == TRUE) encoder.encodeJsonElement(JsonPrimitive(true))
                else encoder.encodeJsonElement(JsonPrimitive(false))
            }

            override fun deserialize(decoder: Decoder): Bool {
                if (decoder.decodeBoolean()) return TRUE
                return FALSE
            }
        }
    }

    @Serializable(Long.Serde::class)
    data class Long(@JvmField val lng: kotlin.Long) : Expr {
        object Serde : KSerializer<Long> {
            override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("xtdb.api.query.Expr.Long", LONG)
            override fun serialize(encoder: Encoder, value: Long) {
                require(encoder is JsonEncoder)
                encoder.encodeJsonElement(JsonPrimitive(value.lng))
            }

            override fun deserialize(decoder: Decoder): Long = `val`(decoder.decodeLong())
        }
    }

    @Serializable(Double.Serde::class)
    data class Double(@JvmField val dbl: kotlin.Double) : Expr {
        object Serde : KSerializer<Double> {
            override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("xtdb.api.query.Expr.Double", DOUBLE)
            override fun serialize(encoder: Encoder, value: Double) {
                require(encoder is JsonEncoder)
                encoder.encodeJsonElement(JsonPrimitive(value.dbl))
            }

            override fun deserialize(decoder: Decoder): Double = `val`(decoder.decodeDouble())
        }
    }

    @Serializable(Obj.Serde::class)
    data class Obj(@JvmField val obj: Any) : Expr {
        object Serde : KSerializer<Obj> {
            @OptIn(ExperimentalSerializationApi::class)
            override val descriptor: SerialDescriptor = SerialDescriptor("xtdb.api.query.Expr.Obj" ,JsonElement.serializer().descriptor)
            override fun serialize(encoder: Encoder, value: Obj) {
                require(encoder is JsonEncoder)
                encoder.encodeJsonElement(encoder.json.encodeToJsonElement<Any>(value.obj))
            }

            override fun deserialize(decoder: Decoder): Obj {
                require(decoder is JsonDecoder)
                return `val`(decoder.json.decodeFromJsonElement<Any>(decoder.decodeJsonElement()))
            }
        }
    }

    @Serializable
    data class LogicVar(@JvmField @SerialName("xt:lvar") val lv: String) : Expr

    @Serializable
    data class Param(@JvmField @SerialName("xt:param") val v: String) : Expr

    @Serializable
    data class Call(@JvmField @SerialName("xt:call") val f: String, @JvmField val args: List<Expr>) : Expr

    @Serializable
    data class Get(@JvmField @SerialName("xt:get") val expr: Expr, @JvmField val field: String) : Expr

    @Serializable
    data class Subquery(@JvmField @SerialName("xt:q") val query: XtqlQuery, @JvmField val args: List<Binding>? = null) : Expr

    @Serializable
    data class Exists(@JvmField @SerialName("xt:exists") val query: XtqlQuery, @JvmField val args: List<Binding>? = null) : Expr

    @Serializable
    data class Pull(@JvmField @SerialName("xt:pull") val query: XtqlQuery, @JvmField val args: List<Binding>? = null) : Expr

    @Serializable
    data class PullMany(
        @JvmField @SerialName("xt:pullMany") val query: XtqlQuery,
        @JvmField val args: List<Binding>? = null,
    ) : Expr

    @Serializable(ListExpr.Serde::class)
    data class ListExpr(@JvmField val elements: List<Expr>) : Expr {
        object Serde : KSerializer<ListExpr> {
            @OptIn(ExperimentalSerializationApi::class)
            override val descriptor: SerialDescriptor =  SerialDescriptor("xtdb.api.query.Expr.ListExpr", JsonArray.serializer().descriptor)

            override fun serialize(encoder: Encoder, value: ListExpr) {
                require(encoder is JsonEncoder)
                encoder.encodeJsonElement(encoder.json.encodeToJsonElement<List<Expr>>(value.elements))
            }

            override fun deserialize(decoder: Decoder): ListExpr {
                require(decoder is JsonDecoder)
                val element = decoder.decodeJsonElement()
                if (element !is JsonArray) throw jsonIAE("xtql/malformed-list-expr", element)
                return list(decoder.json.decodeFromJsonElement<List<Expr>>(element))
            }
        }
    }

    @Serializable(SetExpr.Serde::class)
    data class SetExpr(@JvmField val elements: List<Expr>) : Expr {
        object Serde : KSerializer<SetExpr> {
            @OptIn(ExperimentalSerializationApi::class)
            override val descriptor: SerialDescriptor = SerialDescriptor("xtdb.api.query.Expr.SetExpr", JsonArray.serializer().descriptor)
            override fun serialize(encoder: Encoder, value: SetExpr) {
                require(encoder is JsonEncoder)
                encoder.encodeJsonElement(buildJsonObject {
                    put("@type", "xt:set")
                    put("@value", encoder.json.encodeToJsonElement(value.elements))
                })
            }

            override fun deserialize(decoder: Decoder): SetExpr {
                require(decoder is JsonDecoder)
                val errorType = "xtql/malformed-set-expr"

                val value = decoder.decodeJsonElement()
                    .requireObject(errorType)
                    .requireType(errorType)
                    .requireValue(errorType)
                    .requireArray(errorType)

                return set(decoder.json.decodeFromJsonElement<List<Expr>>(value))
            }
        }
    }

    @Serializable(MapExpr.Serde::class)
    data class MapExpr(@JvmField val elements: Map<String, Expr>) : Expr {
        object Serde: KSerializer<MapExpr> {
            @OptIn(ExperimentalSerializationApi::class)
            override val descriptor: SerialDescriptor = SerialDescriptor("xtdb.api.query.Expr.MapExpr", JsonObject.serializer().descriptor)
            override fun serialize(encoder: Encoder, value: MapExpr) {
                require(encoder is JsonEncoder)
                encoder.encodeJsonElement(encoder.json.encodeToJsonElement(value.elements))
            }

            override fun deserialize(decoder: Decoder): MapExpr {
                require(decoder is JsonDecoder)
                val element = decoder.decodeJsonElement()
                if (element !is JsonObject) throw jsonIAE("xtql/malformed-map-expr", element)
                return map(decoder.json.decodeFromJsonElement<Map<String, Expr>>(element))
            }
        }
    }

    companion object {
        @JvmStatic
        fun `val`(l: kotlin.Long) = Long(l)

        @JvmStatic
        fun `val`(d: kotlin.Double) = Double(d)

        @JvmStatic
        fun `val`(obj: Any) = Obj(obj)

        @JvmStatic
        fun lVar(lv: String) = LogicVar(lv)

        @JvmStatic
        fun param(v: String) = Param(v)

        @JvmStatic
        fun call(f: String, args: List<Expr>) = Call(f, args)

        @JvmStatic
        fun call(f: String, vararg args: Expr) = call(f, args.toList())

        @JvmStatic
        fun get(expr: Expr, field: String) = Get(expr, field)

        @JvmStatic
        fun q(query: XtqlQuery, args: List<Binding>? = null) = Subquery(query, args)

        @JvmStatic
        fun exists(query: XtqlQuery, args: List<Binding>? = null) = Exists(query, args)

        @JvmStatic
        fun pull(query: XtqlQuery, args: List<Binding>? = null) = Pull(query, args)

        @JvmStatic
        fun pullMany(query: XtqlQuery, args: List<Binding>? = null) = PullMany(query, args)

        @JvmStatic
        fun list(elements: List<Expr>) = ListExpr(elements)

        @JvmStatic
        fun list(vararg elements: Expr) = list(elements.toList())

        @JvmStatic
        fun set(elements: List<Expr>) = SetExpr(elements)

        @JvmStatic
        fun set(vararg elements: Expr) = set(elements.toList())

        @JvmStatic
        fun map(elements: Map<String, Expr>) = MapExpr(elements)

        fun map(vararg elements: Pair<String, Expr>) = map(elements.toMap())
    }
}
