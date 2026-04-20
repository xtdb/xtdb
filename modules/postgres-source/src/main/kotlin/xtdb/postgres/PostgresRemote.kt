package xtdb.postgres

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote

class PostgresRemote(
    val hostname: String,
    val port: Int,
    val database: String,
    val username: String,
    val password: String,
) : Remote {

    override fun close() = Unit

    @Serializable
    @SerialName("!Postgres")
    data class Factory(
        val hostname: String,
        val port: Int = 5432,
        val database: String,
        val username: String,
        val password: String,
    ) : Remote.Factory<PostgresRemote> {
        override fun open() = PostgresRemote(hostname, port, database, username, password)
    }

    class Registration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(Factory::class)
        }
    }
}
