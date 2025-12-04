package xtdb.api

import org.apache.arrow.flight.FlightServer.builder
import org.apache.arrow.flight.Location.forGrpcInsecure
import xtdb.flight_sql.XtdbProducer
import xtdb.flight_sql.withErrorLoggingMiddleware
import xtdb.util.closeOnCatch
import xtdb.util.info
import xtdb.util.logger

private val LOGGER = FlightSql::class.logger

interface FlightSql : AutoCloseable {

    val port: Int

    companion object {
        @JvmStatic
        fun open(xtdb: Xtdb, config: FlightSqlConfig): FlightSql {
            XtdbProducer(xtdb).closeOnCatch { producer ->
                val server = builder(xtdb.allocator, forGrpcInsecure(config.host, config.port), producer)
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
}
