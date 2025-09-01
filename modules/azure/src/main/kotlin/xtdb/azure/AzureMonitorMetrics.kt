package xtdb.azure

import io.micrometer.azuremonitor.AzureMonitorConfig
import io.micrometer.azuremonitor.AzureMonitorMeterRegistry
import io.micrometer.core.instrument.Clock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.module.XtdbModule
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.Xtdb

@Serializable
@SerialName("!AzureMonitor")
class AzureMonitorMetrics(
    @Serializable(StringWithEnvVarSerde::class) val connectionString: String,
) : XtdbModule.Factory {

    override val moduleKey = "xtdb.metrics.azure-monitor"

    override fun openModule(xtdb: Xtdb): XtdbModule {
        val reg = AzureMonitorMeterRegistry(
            object : AzureMonitorConfig {
                override fun get(key: String) = null
                override fun connectionString() = connectionString
            },
            Clock.SYSTEM
        )

        xtdb.addMeterRegistry(reg)

        return object : XtdbModule {
            override fun close() {}
        }
    }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<XtdbModule.Factory>) {
            builder.subclass(AzureMonitorMetrics::class)
        }
    }
}