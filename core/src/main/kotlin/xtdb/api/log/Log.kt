@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
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

    sealed interface Message {

        companion object {
            private const val TX_HEADER: Byte = -1
            private const val FLUSH_CHUNK_HEADER: Byte = 2

            fun parse(buffer: ByteBuffer) =
                when (buffer.get(0)) {
                    TX_HEADER -> Tx(buffer.duplicate())
                    FLUSH_CHUNK_HEADER -> FlushChunk(buffer.getLong(1))
                    else -> throw IllegalArgumentException("Unknown message type: ${buffer.get()}")
                }
        }

        class Tx(val payload: ByteBuffer) : Message {
            override fun encode(): ByteBuffer = payload.duplicate()
        }

        data class FlushChunk(val expectedChunkTxId: LogOffset) : Message {
            override fun encode(): ByteBuffer =
                ByteBuffer.allocate(1 + Long.SIZE_BYTES).run {
                    put(FLUSH_CHUNK_HEADER)
                    putLong(expectedChunkTxId)
                    flip()
                }
        }

        fun encode(): ByteBuffer
    }

    interface Factory {
        fun openLog(msgProcessor: Processor?): Log
        fun openFileLog(): FileLog = FileLog.openInMemory()
    }

    val latestSubmittedOffset: LogOffset

    fun appendMessage(message: Message): CompletableFuture<LogOffset>

    class Record(
        val logOffset: LogOffset,
        val logTimestamp: Instant,
        val message: Message
    )

    interface Processor {
        val latestCompletedOffset: LogOffset
        fun processRecords(log: Log, records: List<Record>)
    }
}
