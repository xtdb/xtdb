@file:UseSerializers(IntWithEnvVarSerde::class, InetAddressSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.net.InetAddress
import java.nio.file.Path

@Serializable
data class ServerConfig(
    var host: InetAddress? = InetAddress.getLoopbackAddress(),
    var port: Int = 0,
    var readOnlyPort: Int = -1,
    var numThreads: Int = 42,
    var ssl: SslSettings? = null,
) {

    @Serializable
    data class SslSettings(
        @Serializable(with = PathWithEnvVarSerde::class) val keyStore: Path,
        @Serializable(with = StringWithEnvVarSerde::class) val keyStorePassword: String
    )

    fun host(host: InetAddress?) = apply { this.host = host }

    /**
     * Port on which to start a read-write Postgres wire-compatible server.
     *
     * Default is 0, to have the server choose an available port.
     * Set to -1 to not start a read-write server.
     */
    fun port(port: Int) = apply { this.port = port }

    /**
     * Port on which to start a read-only Postgres wire-compatible server.
     *
     * Default is -1, to not start a read-only server.
     * Set to 0 to have the server choose an available port.
     */
    fun readOnlyPort(readOnlyPort: Int = 0) = apply { this.readOnlyPort = readOnlyPort }

    fun numThreads(numThreads: Int) = apply { this.numThreads = numThreads }

    /**
     * Enable SSL for the PG wire server.
     *
     * @param keyStore path to the keystore file.
     * @param keyStorePassword password for the keystore.
     */
    fun ssl(keyStore: Path, keyStorePassword: String) = apply { ssl = SslSettings(keyStore, keyStorePassword) }
}
