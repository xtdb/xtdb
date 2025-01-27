@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.storage.ObjectStore
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.CompletableFuture

typealias LogOffset = Long

interface Log : AutoCloseable {
    companion object {
        @JvmStatic
        val inMemoryLog get() = InMemoryLog.Factory()

        @JvmStatic
        fun localLog(rootPath: Path) = LocalLog.Factory(rootPath)

        @Suppress("unused")
        @JvmSynthetic
        fun localLog(path: Path, configure: LocalLog.Factory.() -> Unit) = localLog(path).also(configure)
    }

    interface Factory {
        fun openLog(msgProcessor: Processor?): Log
        fun openFileLog(): FileLog = FileLog.openInMemory()
    }

    val latestSubmittedOffset: LogOffset

    fun appendMessage(payload: ByteBuffer): CompletableFuture<LogOffset>

    class Record(
        val logOffset: LogOffset,
        val logTimestamp: Instant,
        val payload: ByteBuffer
    )

    interface Processor {
        val latestCompletedOffset: LogOffset
        fun processRecords(log: Log, records: List<Record>)
    }
}
