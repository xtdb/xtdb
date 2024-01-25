@file:UseSerializers(DurationSerde::class)
package xtdb.api.log

import clojure.lang.IFn
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.Xtdb
import xtdb.api.module.*
import xtdb.util.requiringResolve
import java.time.Duration

object AzureEventHub {
    @JvmStatic
    fun azureEventHub(namespace: String, eventHubName: String) = Factory(namespace, eventHubName)

    private val OPEN_LOG: IFn = requiringResolve("xtdb.azure", "open-log")

    @Serializable
    data class Factory(
        val namespace: String,
        val eventHubName: String,
        var maxWaitTime: Duration = Duration.ofSeconds(1),
        var pollSleepDuration: Duration = Duration.ofSeconds(1),
        var autoCreateEventHub: Boolean = false,
        var retentionPeriodInDays: Int = 7,
        var resourceGroupName: String? = null
    ) : Log.Factory {

        fun maxWaitTime(maxWaitTime: Duration) = apply { this.maxWaitTime = maxWaitTime }
        fun pollSleepDuration(pollSleepDuration: Duration) = apply { this.pollSleepDuration = pollSleepDuration }
        fun autoCreateEventHub(autoCreateEventHub: Boolean) = apply { this.autoCreateEventHub = autoCreateEventHub }
        fun retentionPeriodInDays(retentionPeriodInDays: Int) = apply { this.retentionPeriodInDays = retentionPeriodInDays }
        fun resourceGroupName(resourceGroupName: String) = apply { this.resourceGroupName = resourceGroupName }

        override fun openLog() = OPEN_LOG(this) as Log
    }

    class Registration: XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerLogFactory(Factory::class)
        }
    }
}

@JvmSynthetic
fun Xtdb.Config.azureEventHub(namespace: String, eventHubName: String, configure: AzureEventHub.Factory.() -> Unit = {}) {
    txLog = AzureEventHub.azureEventHub(namespace, eventHubName).also(configure)
}
