package xtdb.mysql

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote

class MySqlRemote(
    val hostname: String,
    val port: Int,
    val username: String,
    val password: String,
) : Remote {

    override fun close() = Unit

    @Serializable
    @SerialName("!MySQL")
    data class Factory @JvmOverloads constructor(
        val hostname: String,
        val port: Int = 3306,
        val username: String,
        val password: String,
    ) : Remote.Factory<MySqlRemote> {
        override fun open() = MySqlRemote(hostname, port, username, password)
    }

    class Registration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(Factory::class)
        }
    }
}
