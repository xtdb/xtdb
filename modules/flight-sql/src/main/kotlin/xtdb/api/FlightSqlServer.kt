@file:JvmName("FlightSqlServer")

package xtdb.api

import xtdb.api.Xtdb.Module
import xtdb.util.requiringResolve

private val OPEN_SERVER = requiringResolve("xtdb.flight-sql", "open-server")

class FlightSqlServerModule(
        var host: String = "127.0.0.1",
        var port: Int = 9832
) : Xtdb.ModuleFactory {
    override val moduleKey = "xtdb.flight-sql-server"

    fun host(host: String) = apply { this.host = host }
    fun port(port: Int) = apply { this.port = port }

    override fun openModule(xtdb: IXtdb) = OPEN_SERVER(xtdb, this) as Module

    companion object {
        @JvmStatic
        fun flightSqlServer() = FlightSqlServerModule()
    }
}

@JvmSynthetic
fun Xtdb.Config.flightSqlServer(configure: FlightSqlServerModule.() -> Unit = {}) {
    modules(FlightSqlServerModule().also(configure))
}
