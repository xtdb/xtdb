package xtdb.api

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.arrow.flight.FlightServer.builder
import org.apache.arrow.flight.Location.forGrpcInsecure
import xtdb.api.module.XtdbModule
import xtdb.flight_sql.XtdbProducer
import xtdb.flight_sql.withErrorLoggingMiddleware
import xtdb.util.closeOnCatch
import xtdb.util.info
import xtdb.util.logger

private val LOGGER = FlightSql::class.logger

interface FlightSql : XtdbModule {

    val port: Int

    @SerialName("!FlightSqlServer")
    @Serializable
    data class Factory(var host: String = "127.0.0.1", var port: Int = 0) : XtdbModule.Factory {
        override val moduleKey = "xtdb.flight-sql-server"

        fun host(host: String) = apply { this.host = host }
        fun port(port: Int) = apply { this.port = port }

        override fun openModule(xtdb: Xtdb): FlightSql {
            XtdbProducer(xtdb).closeOnCatch { producer ->
                val server = builder(xtdb.allocator, forGrpcInsecure(host, port), producer)
                    .also { it.withErrorLoggingMiddleware() }
                    .build()
                    .also { it.start() }

                LOGGER.info("Flight SQL server started, port ${server.port}")

                return object : FlightSql {
                    override val port = server.port

                    override fun close() {
                        server.close()
                        producer.close()
                        LOGGER.info("Flight SQL server stopped")
                    }
                }
            }
        }
    }

    class Registration : XtdbModule.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<XtdbModule.Factory>) {
            builder.subclass(Factory::class)
        }
    }
}

@JvmSynthetic
fun Xtdb.Config.flightSqlServer(configure: FlightSql.Factory.() -> Unit = {}) {
    modules(FlightSql.Factory().also(configure))
}
