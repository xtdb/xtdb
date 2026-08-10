@file:UseSerializers(InetAddressSerde::class)

package xtdb.api.metrics

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.InetAddressSerde
import java.net.InetAddress

@Serializable
data class HealthzConfig(
    var host: InetAddress? = InetAddress.getLoopbackAddress(),

    /**
     * Port to run the healthz server on.
     *
     * Default is 0, to have the server choose an available port - read it back with [xtdb.api.Xtdb.healthzPort].
     * Deployments that need a well-known port (container health checks, Kubernetes probes, Prometheus scrape
     * targets) MUST set it explicitly.
     */
    var port: Int = 0
) {
    fun host(host: InetAddress?) = apply { this.host = host }
    fun port(port: Int) = apply { this.port = port }
}
