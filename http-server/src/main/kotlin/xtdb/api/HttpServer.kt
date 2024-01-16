@file:JvmName("HttpServer")

package xtdb.api

import xtdb.api.Xtdb.Module
import xtdb.util.requiringResolve

private val OPEN_SERVER = requiringResolve("xtdb.server", "open-server")

class HttpServerModule(
    var port: Int = 3000,
    var readOnly: Boolean = false,
) : Xtdb.ModuleFactory {
    override val moduleKey = "xtdb.http-server"

    fun port(port: Int) = apply { this.port = port }
    fun readOnly(readOnly: Boolean) = apply { this.readOnly = readOnly }

    override fun openModule(xtdb: IXtdb) = OPEN_SERVER(xtdb, this) as Module

    companion object {
        @JvmStatic
        fun httpServer() = HttpServerModule()
    }
}

@JvmSynthetic
fun Xtdb.Config.httpServer(configure: HttpServerModule.() -> Unit = {}) {
    modules(HttpServerModule().also(configure))
}
