package xtdb.api.query

import clojure.lang.Keyword
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

private fun String.denormaliseToKeyword(transform: String.() -> String = { this }): Keyword {
    val split = replace(Regex("^_"), "xt\\$").split('$')

    return if (split.size > 1) {
        Keyword.intern(
            split.dropLast(1).joinToString(".", transform = transform),
            split.last().transform()
        )
    } else {
        Keyword.intern(transform())
    }
}

private fun String.kebabCase() = replace(Regex("(?<!^)_"), "-")

@Serializable(IKeyFn.Serde::class)
fun interface IKeyFn<out V> {

    /**
     * @suppress
     */
    object Serde : KSerializer<IKeyFn<Any>> {
        override val descriptor: SerialDescriptor get() = KeyFn.serializer().descriptor

        override fun deserialize(decoder: Decoder): IKeyFn<Any> = decoder.decodeSerializableValue(KeyFn.serializer())

        override fun serialize(encoder: Encoder, value: IKeyFn<Any>) =
            encoder.encodeSerializableValue(
                KeyFn.serializer(),
                value as? KeyFn ?: TODO("error - needs to be KeyFn")
            )
    }

    @Serializable
    enum class KeyFn : IKeyFn<Any> {
        KEBAB_CASE_STRING {
            override fun denormalize(key: String) = key.kebabCase()
        },

        KEBAB_CASE_KEYWORD {
            override fun denormalize(key: String) = key.denormaliseToKeyword(String::kebabCase)
        },

        SNAKE_CASE_STRING {
            override fun denormalize(key: String) = key
        },

        SNAKE_CASE_KEYWORD {
           override fun denormalize(key: String) = key.denormaliseToKeyword()
        },
    }

    fun denormalize(key: String): V
}
