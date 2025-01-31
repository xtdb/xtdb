@file:UseSerializers(DurationSerde::class, IntWithEnvVarSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import java.lang.Runtime.getRuntime

@Serializable
data class CompactorConfig(var threads: Int = DEFAULT_THREADS) {
    companion object {
        private val DEFAULT_THREADS = (getRuntime().availableProcessors() / 2).coerceAtLeast(1)
    }

    fun threads(threads: Int) = apply { this.threads = threads }
}
