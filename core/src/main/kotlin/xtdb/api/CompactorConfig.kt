@file:UseSerializers(DurationSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import java.time.Duration

@Serializable
data class CompactorConfig(var enabled: Boolean = true) {
    fun enabled(enabled: Boolean) = apply { this.enabled = enabled }
}
