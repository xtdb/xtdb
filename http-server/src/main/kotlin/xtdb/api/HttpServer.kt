@file:UseSerializers(IntWithEnvVarSerde::class)

package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.IntWithEnvVarSerde
import xtdb.api.module.XtdbModule
import xtdb.util.requiringResolve

/**
 * Used to set configuration options for an optional HTTP Server module.
 * 
 * Example usage, as part of a node config:
 * ```kotlin
 * Xtdb.openNode {
 *    httpServer() {
 *       port = 3001
 *    },
 *    ...
 * }
 * ```
 */
object HttpServer {
    @JvmStatic
    fun httpServer() = Factory()

    @Serializable
    @SerialName("!HttpServer")
    data class Factory(
        var port: Int = 3000,
    ) : XtdbModule.Factory {
        override val moduleKey = "xtdb.http-server"

        fun port(port: Int) = apply { this.port = port }

        override fun openModule(xtdb: Xtdb) =
            requiringResolve("xtdb.server/open-server")(xtdb, this) as XtdbModule
    }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerModuleFactory(Factory::class)
        }
    }
}

@JvmSynthetic
fun Xtdb.Config.httpServer(configure: HttpServer.Factory.() -> Unit = {}) {
    modules(HttpServer.Factory().also(configure))
}
