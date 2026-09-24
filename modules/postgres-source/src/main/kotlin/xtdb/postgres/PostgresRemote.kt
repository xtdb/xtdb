package xtdb.postgres

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote
import kotlin.time.Duration

class PostgresRemote(
    val hostname: String,
    val port: Int,
    val database: String,
    val username: String,
    val password: String,
    val statusInterval: Duration?,
) : Remote {

    override fun close() = Unit

    @Serializable
    @SerialName("!Postgres")
    data class Factory @JvmOverloads constructor(
        val hostname: String,
        val port: Int = 5432,
        val database: String,
        val username: String,
        val password: String,
        @Transient val statusInterval: Duration? = null,
    ) : Remote.Factory<PostgresRemote> {
        override fun open() = PostgresRemote(hostname, port, database, username, password, statusInterval)
    }

    class Registration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(Factory::class)
        }
    }
}
