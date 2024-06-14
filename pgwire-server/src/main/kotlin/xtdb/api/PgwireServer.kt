package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import xtdb.api.module.XtdbModule
import xtdb.util.requiringResolve

object PgwireServer {

    @JvmStatic
    fun pgwireServer() = Factory()

    @SerialName("!PgwireServer")
    @Serializable
    data class Factory(
        var port: Int = 5432,
        var numThreads: Int = 42,
    ) : XtdbModule.Factory {
        override val moduleKey = "xtdb.pgwire-server"

        /**
         * Port to start the Pgwire server on. Default is 5432.
         *
         * Specify '0' to have the server choose an available port.
         */
        fun port(port: Int) = apply { this.port = port }
        fun numThreads(numThreads: Int) = apply { this.numThreads = numThreads }

        override fun openModule(xtdb: IXtdb): XtdbModule =
            requiringResolve("xtdb.pgwire/open-server")(xtdb, this) as XtdbModule
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
fun Xtdb.Config.pgwireServer(configure: PgwireServer.Factory.() -> Unit = {}) {
    modules(PgwireServer.Factory().also(configure))
}
