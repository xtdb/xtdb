@file:UseSerializers(StringWithEnvVarSerde::class, PathWithEnvVarSerde::class, DurationSerde::class)

package xtdb.api.log

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.Xtdb
import xtdb.api.module.XtdbModule
import xtdb.util.requiringResolve
import java.time.Duration

object AzureEventHub {
    @JvmStatic
    fun azureEventHub(namespace: String, eventHubName: String) = Factory(namespace, eventHubName)

    @Serializable
    @SerialName("!Azure")
    data class Factory(
        val namespace: String,
        val eventHubName: String,
        var maxWaitTime: Duration = Duration.ofSeconds(1),
        var pollSleepDuration: Duration = Duration.ofSeconds(1),
        var autoCreateEventHub: Boolean = false,
        var retentionPeriodInDays: Int = 7,
        var resourceGroupName: String? = null,
        var userManagedIdentityClientId: String? = null,
    ) : Log.Factory {

        fun maxWaitTime(maxWaitTime: Duration) = apply { this.maxWaitTime = maxWaitTime }
        fun pollSleepDuration(pollSleepDuration: Duration) = apply { this.pollSleepDuration = pollSleepDuration }
        fun autoCreateEventHub(autoCreateEventHub: Boolean) = apply { this.autoCreateEventHub = autoCreateEventHub }
        fun retentionPeriodInDays(retentionPeriodInDays: Int) =
            apply { this.retentionPeriodInDays = retentionPeriodInDays }

        fun resourceGroupName(resourceGroupName: String) = apply { this.resourceGroupName = resourceGroupName }
        fun userManagedIdentityClientId(userManagedIdentityClientId: String) = apply { this.userManagedIdentityClientId = userManagedIdentityClientId }

        override fun openLog() = requiringResolve("xtdb.azure/open-log")(this) as Log
    }

    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerLogFactory(Factory::class)
        }
    }
}

@JvmSynthetic
fun Xtdb.Config.azureEventHub(namespace: String, eventHubName: String, configure: AzureEventHub.Factory.() -> Unit = {}) {
    txLog = AzureEventHub.azureEventHub(namespace, eventHubName).also(configure)
}
