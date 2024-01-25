package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import xtdb.api.module.XtdbModule
import xtdb.util.requiringResolve

object PgwireServer {
    private val OPEN_SERVER = requiringResolve("xtdb.pgwire", "open-server")

    @JvmStatic
    fun pgwireServer() = Factory()

    @SerialName("!PgwireServer")
    @Serializable
    data class Factory(
        var port: Int = 5432,
        var numThreads: Int = 42,
    ) : XtdbModule.Factory {
        override val moduleKey = "xtdb.pgwire-server"

        fun port(port: Int) = apply { this.port = port }
        fun numThreads(numThreads: Int) = apply { this.numThreads = numThreads }

        override fun openModule(xtdb: IXtdb) = OPEN_SERVER(xtdb, this) as XtdbModule

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
