package xtdb.api.query

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import xtdb.api.query.Exprs.lVar
import xtdb.jsonIAE

internal object BindingSerde : KSerializer<Binding> {
    @OptIn(ExperimentalSerializationApi::class, InternalSerializationApi::class)
    override val descriptor: SerialDescriptor =
        buildSerialDescriptor("xtdb.api.query.Binding", PolymorphicKind.SEALED) {
            element("shortForm", PrimitiveSerialDescriptor("xtdb.api.query.Binding", PrimitiveKind.STRING))
            element("longForm",
                buildSerialDescriptor("xtdb.api.query.Binding", StructureKind.MAP) {
                    element<String>("binding")
                    element("expr", Expr.serializer().descriptor)
                }
            )
        }

    override fun serialize(encoder: Encoder, value: Binding) {
        require(encoder is JsonEncoder)
        encoder.encodeJsonElement(buildJsonObject {
            put(
                value.binding,
                encoder.json.encodeToJsonElement<Expr>(value.expr)
            )
        })
    }

    override fun deserialize(decoder: Decoder): Binding {
        require(decoder is JsonDecoder)

        return when (val element = decoder.decodeJsonElement()) {
            is JsonPrimitive ->
                if (!element.isString) throw jsonIAE("xtql/malformed-binding", element)
                else Binding(element.content)

            is JsonObject -> {
                if (element.entries.size != 1) throw jsonIAE("xtql/malformed-binding", element)
                val (binding, expr) = element.entries.first()
                Binding(binding, decoder.json.decodeFromJsonElement<Expr>(expr))
            }

            else -> throw jsonIAE("xtql/malformed-binding", element)
        }
    }
}

@Serializable(BindingSerde::class)
data class Binding(val binding: String, val expr: Expr) {
    @JvmOverloads
    constructor(binding: String, bindVar: String = binding) : this(binding, lVar(bindVar))
    constructor(binding: Pair<String, Expr>) : this(binding.first, binding.second)

    /**
     * @suppress
     */
    abstract class ABuilder<B : ABuilder<B, O>, O> {
        private val bindings: MutableList<Binding> = mutableListOf()

        @Suppress("UNCHECKED_CAST")
        fun setBindings(bindings: Collection<Binding>) = (this as B).apply {
            this.bindings.clear()
            this.bindings += bindings
        }

        @Suppress("UNCHECKED_CAST")
        fun bind(binding: String, expr: Expr): B = (this as B).apply { bindings += Binding(binding, expr) }

        @JvmOverloads
        fun bind(binding: String, varName: String = binding) = bind(binding, lVar(varName))

        @JvmSynthetic
        @JvmName("bindAll_str_str")
        fun bindAll(vararg bindings: Pair<String, String>) =
            this.apply { bindings.mapTo(this.bindings) { Binding(it.first, it.second) } }

        @JvmSynthetic
        fun bindAll(vararg bindings: Pair<String, Expr>) =
            this.apply { bindings.mapTo(this.bindings, ::Binding) }

        @Suppress("UNCHECKED_CAST")
        fun bindAll(vararg cols: String): B = (this as B).apply {
            cols.mapTo(bindings) { Binding(it, lVar(it)) }
        }

        protected fun getBindings(): List<Binding> = bindings

        abstract fun build(): O
    }

    class Builder : ABuilder<Builder, List<Binding>>() {
        override fun build() = getBindings()
    }
}
