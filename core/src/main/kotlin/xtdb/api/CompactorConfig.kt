@file:UseSerializers(DurationSerde::class, IntWithEnvVarSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import java.lang.Runtime.getRuntime

private val DEFAULT_THREADS = (getRuntime().availableProcessors() / 2).coerceAtLeast(1)

@Serializable
data class CompactorConfig(
    var threads: Int = System.getenv("XTDB_COMPACTOR_THREADS")?.toIntOrNull() ?: DEFAULT_THREADS
) {
    fun threads(threads: Int) = apply { this.threads = threads }
}
