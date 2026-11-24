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
    var optIn221: Boolean = System.getenv("XTDB_COMPACTOR_221_OPT_IN").toBoolean(),
) {
    fun threads(threads: Int) = apply { this.threads = threads }
    fun optIn221(optIn221: Boolean) = apply { this.optIn221 = optIn221 }
}
