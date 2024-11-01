package xtdb.api

import kotlinx.serialization.Serializable
import java.nio.file.Path

@Serializable
data class ServerConfig(
    var port: Int = 5432,
    var numThreads: Int = 42,
    var ssl: SslSettings? = null,
) {

    @Serializable
    data class SslSettings(
        @Serializable(with = PathWithEnvVarSerde::class) val keyStore: Path,
        @Serializable(with = StringWithEnvVarSerde::class) val keyStorePassword: String
    )

    /**
     * Port to start the Pgwire server on. Default is 5432.
     *
     * Specify '0' to have the server choose an available port.
     */
    fun port(port: Int) = apply { this.port = port }

    fun anyAvailablePort() = port(0)

    fun numThreads(numThreads: Int) = apply { this.numThreads = numThreads }

    /**
     * Enable SSL for the Pgwire server.
     *
     * @param keyStore path to the keystore file.
     * @param keyStorePassword password for the keystore.
     */
    fun ssl(keyStore: Path, keyStorePassword: String) = apply { ssl = SslSettings(keyStore, keyStorePassword) }
}