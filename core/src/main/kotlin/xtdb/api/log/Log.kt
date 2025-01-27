@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.log.LogMessage
import xtdb.log.LogMessage.MessageCase
import xtdb.log.flushChunk
import xtdb.log.logMessage
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
            private const val LEGACY_FLUSH_CHUNK_HEADER: Byte = 2
            private const val PROTOBUF_HEADER: Byte = 3

            fun parse(buffer: ByteBuffer) =
                when (buffer.get(0)) {
                    TX_HEADER -> Tx(buffer.duplicate())
                    LEGACY_FLUSH_CHUNK_HEADER -> FlushChunk(buffer.getLong(1))

                    PROTOBUF_HEADER -> {
                        val protoMsg = LogMessage.parseFrom(buffer.duplicate().position(1))

                        when (val msgCase = protoMsg.messageCase) {
                            MessageCase.FLUSH_CHUNK -> FlushChunk(protoMsg.flushChunk.expectedChunkTxId)
                            else -> throw IllegalArgumentException("Unknown protobuf message type: $msgCase")
                        }
                    }

                    else -> throw IllegalArgumentException("Unknown message type: ${buffer.get()}")
                }
        }

        class Tx(val payload: ByteBuffer) : Message {
            override fun encode(): ByteBuffer = payload.duplicate()
        }

        data class FlushChunk(val expectedChunkTxId: LogOffset) : Message {
            override fun encode(): ByteBuffer {
                val msg = logMessage {
                    flushChunk = flushChunk { this.expectedChunkTxId = this@FlushChunk.expectedChunkTxId }
                }

                return ByteBuffer.allocate(1 + msg.serializedSize).run {
                    put(PROTOBUF_HEADER)
                    put(msg.toByteArray())
                    flip()
                }
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
