@file:UseSerializers(DurationSerde::class)
package xtdb.api
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import java.time.Duration

@Serializable
data class GarbageCollectorConfig(
    /**
     * Gates the leader's auto-signal at block boundaries; explicit `awaitNoGarbage()` always runs.
     * Defaults to `false` — opt-in for now while the leader-only flow gets some real-world miles.
     */
    var enabled: Boolean = false,

    var blocksToKeep: Int = 10,
    var garbageLifetime: Duration = Duration.ofHours(24),
) {
    fun enabled(enabled: Boolean) = apply { this.enabled = enabled }
    fun blocksToKeep(blocksToKeep: Int) = apply { this.blocksToKeep = blocksToKeep }
    fun garbageLifetime(garbageLifetime: Duration) = apply { this.garbageLifetime = garbageLifetime }
}
