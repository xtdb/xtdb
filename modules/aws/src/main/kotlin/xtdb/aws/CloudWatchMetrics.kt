package xtdb.aws

import io.micrometer.cloudwatch2.CloudWatchConfig
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import xtdb.api.metrics.Metrics
import xtdb.api.module.XtdbModule
import xtdb.util.requiringResolve

class CloudWatchMetrics(override val registry: MeterRegistry) : Metrics {

    @Serializable
    @SerialName("!CloudWatch")
    data class Factory (
        @Serializable val namespace: String = "xtdb.metrics",
        @Transient val client: CloudWatchAsyncClient = CloudWatchAsyncClient.create()
    ): Metrics.Factory {
        override fun openMetrics(): Metrics {
            val config = object : CloudWatchConfig {
                override fun get(key: String) = null
                override fun namespace() = namespace
            }

            return CloudWatchMetrics(CloudWatchMeterRegistry(config, Clock.SYSTEM, client))
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
