package xtdb.aws

import io.micrometer.cloudwatch2.CloudWatchConfig
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry
import io.micrometer.core.instrument.Clock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import xtdb.api.Xtdb
import xtdb.api.module.XtdbModule

@Serializable
@SerialName("!CloudWatch")
class CloudWatchMetrics(
    val namespace: String = "xtdb.metrics",
    @Transient val client: CloudWatchAsyncClient = CloudWatchAsyncClient.create()
) : XtdbModule.Factory {
    override val moduleKey = "xtdb.metrics.cloudwatch"

    override fun openModule(xtdb: Xtdb): XtdbModule {
        val config = object : CloudWatchConfig {
            override fun get(key: String) = null
            override fun namespace() = namespace
        }

        xtdb.addMeterRegistry(CloudWatchMeterRegistry(config, Clock.SYSTEM, client))

        return object : XtdbModule {
            override fun close() {}
        }
    }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerModuleFactory(CloudWatchMetrics::class)
        }
    }
}
