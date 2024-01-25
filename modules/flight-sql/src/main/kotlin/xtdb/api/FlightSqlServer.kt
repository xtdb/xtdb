@file:JvmName("FlightSqlServer")

package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import xtdb.api.module.Module
import xtdb.api.module.ModuleFactory
import xtdb.api.module.ModuleRegistration
import xtdb.api.module.ModuleRegistry
import xtdb.util.requiringResolve

private val OPEN_SERVER = requiringResolve("xtdb.flight-sql", "open-server")

@SerialName("!FlightSqlServer")
@Serializable
data class FlightSqlServerModule(
        var host: String = "127.0.0.1",
        var port: Int = 9832
) : ModuleFactory {
    override val moduleKey = "xtdb.flight-sql-server"

    fun host(host: String) = apply { this.host = host }
    fun port(port: Int) = apply { this.port = port }

    override fun openModule(xtdb: IXtdb) = OPEN_SERVER(xtdb, this) as Module

    companion object {
        @JvmStatic
        fun flightSqlServer() = FlightSqlServerModule()
    }

    class Registration: ModuleRegistration {
        override fun register(registry: ModuleRegistry) {
            registry.registerModuleFactory(FlightSqlServerModule::class)
        }
    }
}

@JvmSynthetic
fun Xtdb.Config.flightSqlServer(configure: FlightSqlServerModule.() -> Unit = {}) {
    modules(FlightSqlServerModule().also(configure))
}
