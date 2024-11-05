package xtdb.api.metrics

import kotlinx.serialization.Serializable

@Serializable
data class HealthzConfig(var port: Int = 8080) {
    fun port(port: Int) = apply { this.port = port }
}
