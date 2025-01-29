@file:UseSerializers(IntWithEnvVarSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.nio.file.Path

@Serializable
data class ServerConfig(
    var port: Int = 0,
    var numThreads: Int = 42,
    var ssl: SslSettings? = null,
) {

    @Serializable
    data class SslSettings(
        @Serializable(with = PathWithEnvVarSerde::class) val keyStore: Path,
        @Serializable(with = StringWithEnvVarSerde::class) val keyStorePassword: String
    )

    /**
     * Port to start the PG wire server on.
     *
     * Default is 0, to have the server choose an available port.
     */
    fun port(port: Int) = apply { this.port = port }

    fun numThreads(numThreads: Int) = apply { this.numThreads = numThreads }

    /**
     * Enable SSL for the PG wire server.
     *
     * @param keyStore path to the keystore file.
     * @param keyStorePassword password for the keystore.
     */
    fun ssl(keyStore: Path, keyStorePassword: String) = apply { ssl = SslSettings(keyStore, keyStorePassword) }
}
