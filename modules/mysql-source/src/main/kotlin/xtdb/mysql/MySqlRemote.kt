package xtdb.mysql

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class MySqlRemote(
    val hostname: String,
    val port: Int,
    val username: String,
    val password: String,
    val heartbeatInterval: Duration,
) : Remote {

    override fun close() = Unit

    @Serializable
    @SerialName("!MySQL")
    data class Factory @JvmOverloads constructor(
        val hostname: String,
        val port: Int = 3306,
        val username: String,
        val password: String,
        @Transient val heartbeatInterval: Duration = 10.seconds,
    ) : Remote.Factory<MySqlRemote> {
        override fun open() = MySqlRemote(hostname, port, username, password, heartbeatInterval)
    }

    class Registration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(Factory::class)
        }
    }
}
