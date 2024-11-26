@file:UseSerializers(IntWithEnvVarSerde::class)

package xtdb.api.metrics

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.IntWithEnvVarSerde

@Serializable
data class HealthzConfig(var port: Int = 8080) {
    fun port(port: Int) = apply { this.port = port }
}
