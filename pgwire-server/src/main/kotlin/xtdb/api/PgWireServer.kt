@file:JvmName("PgWireServer")

package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import xtdb.api.module.Module
import xtdb.api.module.ModuleFactory
import xtdb.api.module.ModuleRegistration
import xtdb.api.module.ModuleRegistry
import xtdb.util.requiringResolve

private val OPEN_SERVER = requiringResolve("xtdb.pgwire", "open-server")

@SerialName("!PgwireServer")
@Serializable
data class PgwireServerModule(
        var port: Int = 5432,
        var numThreads: Int = 42
) : ModuleFactory {
    override val moduleKey = "xtdb.pgwire-server"

    fun port(port: Int) = apply { this.port = port }
    fun numThreads(numThreads: Int) = apply { this.numThreads = numThreads }
    
    override fun openModule(xtdb: IXtdb) = OPEN_SERVER(xtdb, this) as Module

    companion object {
        @JvmStatic
        fun pgwireServer() = PgwireServerModule()
    }

    class Registration: ModuleRegistration {
        override fun register(registry: ModuleRegistry) {
            registry.registerModuleFactory(PgwireServerModule::class)
        }
    }
}

@JvmSynthetic
fun Xtdb.Config.pgwireServer(configure: PgwireServerModule.() -> Unit = {}) {
    modules(PgwireServerModule().also(configure))
}
