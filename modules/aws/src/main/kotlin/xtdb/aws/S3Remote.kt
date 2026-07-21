package xtdb.aws

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote

class S3Remote(
    val accessKey: String,
    val secretKey: String,
) : Remote {
    override fun close() = Unit

    @Serializable
    @SerialName("!S3")
    data class Factory(
        val accessKey: String,
        val secretKey: String,
    ) : Remote.Factory<S3Remote> {
        override fun open() = S3Remote(accessKey, secretKey)
    }

    class Registration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(Factory::class)
        }
    }
}
