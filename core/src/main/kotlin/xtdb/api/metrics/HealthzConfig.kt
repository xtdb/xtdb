@file:UseSerializers(IntWithEnvVarSerde::class, InetAddressSerde::class)

package xtdb.api.metrics

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.InetAddressSerde
import xtdb.api.IntWithEnvVarSerde
import java.net.InetAddress

@Serializable
data class HealthzConfig(
    var host: InetAddress? = InetAddress.getLoopbackAddress(),
    var port: Int = 8080
) {
    fun host(host: InetAddress?) = apply { this.host = host }
    fun port(port: Int) = apply { this.port = port }
}
