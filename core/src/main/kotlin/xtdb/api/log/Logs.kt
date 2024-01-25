@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import clojure.lang.IFn
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.util.requiringResolve
import java.nio.file.Path
import java.time.Duration
import java.time.InstantSource

private val OPEN_IN_MEMORY_LOG: IFn = requiringResolve("xtdb.log.memory-log", "open-log")
private val OPEN_LOCAL_LOG: IFn = requiringResolve("xtdb.log.local-directory-log", "open-log")

object Logs {
    @SerialName("!InMemory")
    @Serializable
    data class InMemoryLogFactory(@Transient var instantSource: InstantSource = InstantSource.system()) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }

        override fun openLog() = OPEN_IN_MEMORY_LOG(this) as Log
    }

    @JvmStatic
    fun inMemoryLog() = InMemoryLogFactory()

    /**
     * Used to set configuration options for a local directory based XTDB Transaction Log.
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.Config(
     *    log = LocalLogFactory(
     *              path = Paths.get("test-path"),
     *              instantSource = InstantSource.system(),
     *              bufferSize = 4096,
     *              pollSleepDuration = Duration.ofMillis(100)
     *          ),
     *    ...
     * )
     * ```
     */
    @SerialName("!Local")
    @Serializable
    data class LocalLogFactory @JvmOverloads constructor(
        val path: Path,
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var bufferSize: Long = 4096,
        var pollSleepDuration: Duration = Duration.ofMillis(100),
    ) : Log.Factory {

        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun bufferSize(bufferSize: Long) = apply { this.bufferSize = bufferSize }
        fun pollSleepDuration(pollSleepDuration: Duration) = apply { this.pollSleepDuration = pollSleepDuration }

        override fun openLog() = OPEN_LOCAL_LOG(this) as Log
    }

    @JvmStatic
    fun localLog(path: Path) = LocalLogFactory(path)

    @JvmSynthetic
    fun localLog(path: Path, configure: LocalLogFactory.() -> Unit) = localLog(path).also(configure)
}
