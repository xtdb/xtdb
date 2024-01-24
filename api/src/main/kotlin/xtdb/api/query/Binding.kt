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

    /**
     * @suppress
     */
    abstract class ABuilder<B : ABuilder<B, O>, O> {
        private val bindings: MutableList<Binding> = mutableListOf()

        fun setBindings(bindings: Collection<Binding>) = this.apply {
            this.bindings.clear()
            this.bindings.addAll(bindings)
        }

        @Suppress("UNCHECKED_CAST")
        fun bind(binding: String, expr: Expr): B = this.apply { bind(Binding(binding, expr)) } as B

        fun bind(binding: String, varName: String): B = bind(binding, lVar(varName))

        @Suppress("UNCHECKED_CAST")
        fun bind(binding: Binding): B = this.apply { bindings += binding } as B

        @JvmSynthetic
        infix fun String.boundTo(expr: Expr) = bind(this, expr)

        @JvmSynthetic
        infix fun String.boundTo(varName: String) = bind(this, lVar(varName))

        @JvmSynthetic
        operator fun String.unaryPlus() = this boundTo this

        @JvmSynthetic
        @JvmName("bindCols")
        operator fun Collection<String>.unaryPlus() = this.apply { +map { Binding(it, lVar(it)) } }

        @JvmSynthetic
        @JvmName("bind")
        operator fun Collection<Binding>.unaryPlus() = this.apply { bindings += this }

        @JvmSynthetic
        operator fun Binding.unaryPlus() = this.apply { bindings += this }

        @Suppress("UNCHECKED_CAST")
        fun bindCols(vararg cols: String): B = this.apply { +cols.toList() } as B

        protected fun buildBindings(): List<Binding> = bindings

        abstract fun build(): O
    }

    class Builder : ABuilder<Builder, List<Binding>>() {
        override fun build() = buildBindings()
    }
}
