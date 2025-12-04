@file:UseSerializers(IntWithEnvVarSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers

@Serializable
data class FlightSqlConfig(
    var host: String = "127.0.0.1",
    var port: Int = 0,
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
}
