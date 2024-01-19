package xtdb.api.log

import clojure.lang.IFn
import xtdb.api.TransactionKey
import xtdb.util.requiringResolve
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Duration
import java.time.InstantSource
import java.util.concurrent.CompletableFuture

data class AzureEventHubLogFactory @JvmOverloads constructor(
    val namespace: String,
    val eventHubName: String,
    var maxWaitTime: Duration = Duration.ofSeconds(1),
    var pollSleepDuration: Duration = Duration.ofSeconds(1),
    var autoCreateEventHub: Boolean = false, 
    var retentionPeriodInDays: Int = 7,
    var resourceGroupName: String? = null
) : LogFactory {

    companion object {
        private val OPEN_LOG: IFn = requiringResolve("xtdb.azure", "open-log")
    }

    fun maxWaitTime(maxWaitTime: Duration) = apply { this.maxWaitTime = maxWaitTime }
    fun pollSleepDuration(pollSleepDuration: Duration) = apply { this.pollSleepDuration = pollSleepDuration }
    fun autoCreateEventHub(autoCreateEventHub: Boolean) = apply { this.autoCreateEventHub = autoCreateEventHub }
    fun retentionPeriodInDays(retentionPeriodInDays: Int) = apply { this.retentionPeriodInDays = retentionPeriodInDays }
    fun resourceGroupName(resourceGroupName: String) = apply { this.resourceGroupName = resourceGroupName }

    override fun openLog() = OPEN_LOG(this) as Log
}
