package xtdb.azure

import io.micrometer.azuremonitor.AzureMonitorConfig
import io.micrometer.azuremonitor.AzureMonitorMeterRegistry
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import xtdb.api.metrics.Metrics
import xtdb.api.module.XtdbModule
import xtdb.api.StringWithEnvVarSerde

class AzureMonitorMetrics(override val registry: MeterRegistry) : Metrics {

    @Serializable
    @SerialName("!AzureMonitor")
    data class Factory (
        @Serializable(StringWithEnvVarSerde::class) val instrumentationKey: String = "xtdb.metrics",
    ): Metrics.Factory {
        override fun openMetrics(): Metrics {
            val config = object : AzureMonitorConfig {
                override fun get(key: String) = null
                override fun instrumentationKey() = instrumentationKey
            }
            return AzureMonitorMetrics(AzureMonitorMeterRegistry(config, Clock.SYSTEM))
        }
    }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerMetricsFactory(Factory::class)
        }
    }
}