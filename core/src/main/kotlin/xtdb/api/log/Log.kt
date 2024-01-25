@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)
package xtdb.api.log

import clojure.lang.IFn
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.TransactionKey
import xtdb.util.requiringResolve
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Duration
import java.time.InstantSource
import java.util.concurrent.CompletableFuture

data class LogRecord(val txKey: TransactionKey, val record: ByteBuffer)

interface LogSubscriber {
    fun onSubscribe(closeHook: AutoCloseable)
    fun acceptRecord(record: LogRecord)
}

interface Log : AutoCloseable {
    fun appendRecord(record: ByteBuffer): CompletableFuture<TransactionKey>
    fun readRecords(afterTxId: Long?, limit: Int): List<LogRecord>
    fun subscribe(afterTxId: Long?, subscriber: LogSubscriber)

    /**
     * @suppress
     */
    override fun close() {
    }
}

interface LogFactory {
    fun openLog(): Log
}

@SerialName("!InMemory")
@Serializable
data class InMemoryLogFactory(@Transient var instantSource: InstantSource = InstantSource.system()) : LogFactory {
    private companion object {
        private val OPEN_LOG: IFn = requiringResolve("xtdb.log.memory-log", "open-log")
    }

    fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }

    override fun openLog() = OPEN_LOG(this) as Log
}

@SerialName("!Local")
@Serializable
data class LocalLogFactory @JvmOverloads constructor(
    val path: Path,
    @Transient var instantSource: InstantSource = InstantSource.system(),
    var bufferSize: Long = 4096,
    var pollSleepDuration: Duration = Duration.ofMillis(100),
) : LogFactory {

    private companion object {
        private val OPEN_LOG: IFn = requiringResolve("xtdb.log.local-directory-log", "open-log")
    }

    fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
    fun bufferSize(bufferSize: Long) = apply { this.bufferSize = bufferSize }
    fun pollSleepDuration(pollSleepDuration: Duration) = apply { this.pollSleepDuration = pollSleepDuration }

    override fun openLog() = OPEN_LOG(this) as Log
}

object Logs {
    @JvmStatic
    fun inMemoryLog() = InMemoryLogFactory()

    @JvmStatic
    fun localLog(path: Path) = LocalLogFactory(path)

    @JvmSynthetic
    fun localLog(path: Path, configure: LocalLogFactory.() -> Unit) = localLog(path).also(configure)
}
