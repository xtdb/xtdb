@file:UseSerializers(DurationSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import java.lang.Runtime.getRuntime

private val DEFAULT_THREADS = (getRuntime().availableProcessors() / 2).coerceAtLeast(1)

@Serializable
data class CompactorConfig(
    var threads: Int = System.getenv("XTDB_COMPACTOR_THREADS")?.toIntOrNull() ?: DEFAULT_THREADS,
    var reeIids: Boolean = System.getenv("XTDB_REE_IIDS").toBoolean(),
    // TODO(#6263): goes with withinFileRecencyBranching
    var withinFileRecencyBranching: Boolean = System.getenv("XTDB_WITHIN_FILE_RECENCY_BRANCHING").toBoolean(),
) {
    fun threads(threads: Int) = apply { this.threads = threads }
    fun reeIids(reeIids: Boolean) = apply { this.reeIids = reeIids }

    // TODO(#6263): goes with withinFileRecencyBranching
    fun withinFileRecencyBranching(withinFileRecencyBranching: Boolean) =
        apply { this.withinFileRecencyBranching = withinFileRecencyBranching }
}
