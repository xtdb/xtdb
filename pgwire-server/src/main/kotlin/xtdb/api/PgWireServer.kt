@file:JvmName("PgWireServer")

package xtdb.api

import xtdb.api.Xtdb.Module
import xtdb.util.requiringResolve

private val OPEN_SERVER = requiringResolve("xtdb.pgwire", "open-server")

class PgwireServerModule(
        var port: Int = 5432,
        var numThreads: Int = 42
) : Xtdb.ModuleFactory {
    override val moduleKey = "xtdb.pgwire-server"

    fun port(port: Int) = apply { this.port = port }
    fun numThreads(numThreads: Int) = apply { this.numThreads = numThreads }
    
    override fun openModule(xtdb: IXtdb) = OPEN_SERVER(xtdb, this) as Module

    companion object {
        @JvmStatic
        fun pgwireServer() = PgwireServerModule()
    }
}

@JvmSynthetic
fun Xtdb.Config.pgwireServer(configure: PgwireServerModule.() -> Unit = {}) {
    modules(PgwireServerModule().also(configure))
}
