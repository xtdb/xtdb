@file:UseSerializers(DurationSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import java.time.Duration

@Serializable
data class FlightSqlConfig(
    var host: String = "127.0.0.1",
    var port: Int = 0,
    var transactionIdleTimeout: Duration = Duration.ofMinutes(30),
    var preparedStatementIdleTimeout: Duration = Duration.ofMinutes(30),
) {
    /**
     * Host on which to start the Flight SQL server.
     *
     * Default is "127.0.0.1" (localhost).
     */
    fun host(host: String) = apply { this.host = host }

    /**
     * Port on which to start the Flight SQL server.
     *
     * Default is 0, to have the server choose an available port.
     * Set to -1 to not start a Flight SQL server.
     */
    fun port(port: Int) = apply { this.port = port }

    /**
     * How long a Flight SQL transaction may go without a call naming its handle before it is rolled back.
     * Advertised to clients as `FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT`.
     *
     * Default is 30 minutes.
     */
    fun transactionIdleTimeout(timeout: Duration) = apply { this.transactionIdleTimeout = timeout }

    /**
     * How long a prepared statement may go without a call naming its handle before it is closed.
     * Advertised to clients as `FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT`.
     *
     * Default is 30 minutes.
     */
    fun preparedStatementIdleTimeout(timeout: Duration) = apply { this.preparedStatementIdleTimeout = timeout }
}
