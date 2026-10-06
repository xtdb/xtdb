package xtdb.api

import org.apache.arrow.flight.FlightServer.builder
import org.apache.arrow.flight.Location.forGrpcInsecure
import xtdb.flight_sql.XtdbProducer
import xtdb.flight_sql.withDatabaseMiddleware
import xtdb.flight_sql.withErrorLoggingMiddleware
import xtdb.flight_sql.withSessionMiddleware
import xtdb.api.error.Incorrect
import xtdb.util.closeOnCatch
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn
import java.time.Duration
import java.time.InstantSource
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

private val LOGGER = FlightSql::class.logger

interface FlightSql : AutoCloseable {

    val port: Int

    companion object {
        @JvmStatic
        @JvmOverloads
        fun open(
            xtdb: Xtdb,
            config: FlightSqlConfig,
            clock: InstantSource = InstantSource.system(),
            sweepInterval: Duration = Duration.ofSeconds(10),
        ): FlightSql {
            for ((key, timeout) in listOf(
                "transactionIdleTimeout" to config.transactionIdleTimeout,
                "preparedStatementIdleTimeout" to config.preparedStatementIdleTimeout,
            )) {
                if (timeout <= Duration.ZERO)
                    throw Incorrect(
                        "flightSql.$key must be positive, got $timeout",
                        "xtdb.flight-sql/invalid-idle-timeout", mapOf("key" to key, "timeout" to timeout.toString())
                    )
            }

            require(sweepInterval > Duration.ZERO) { "sweepInterval must be positive, got $sweepInterval" }

            XtdbProducer(xtdb, config, clock).closeOnCatch { producer ->
                val host = if (config.host == "*") "0.0.0.0" else config.host
                val server = builder(xtdb.allocator, forGrpcInsecure(host, config.port), producer)
                    .also { it.withErrorLoggingMiddleware() }
                    .also { it.withDatabaseMiddleware() }
                    .also { it.withSessionMiddleware() }
                    .build()
                    .also { it.start() }

                val sweeper = Executors.newSingleThreadScheduledExecutor { r ->
                    Thread(r, "flight-sql-sweeper").also { it.isDaemon = true }
                }

                sweeper.scheduleWithFixedDelay(
                    {
                        try {
                            producer.sweep()
                        } catch (t: Throwable) {
                            LOGGER.warn(t, "Flight SQL sweep failed")
                        }
                    },
                    sweepInterval.toMillis(), sweepInterval.toMillis(), TimeUnit.MILLISECONDS
                )

                LOGGER.info("Flight SQL server started, port ${server.port}")

                return object : FlightSql {
                    override val port = server.port

                    override fun close() {
                        sweeper.shutdown()
                        sweeper.awaitTermination(10, TimeUnit.SECONDS)
                        server.close()
                        producer.close()
                        LOGGER.info("Flight SQL server stopped")
                    }
                }
            }
        }
    }
}
