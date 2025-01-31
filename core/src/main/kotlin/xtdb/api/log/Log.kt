@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.log.*
import xtdb.log.proto.*
import xtdb.log.proto.LogMessage.MessageCase
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

        fun encode(): ByteBuffer

        companion object {
            private const val TX_HEADER: Byte = -1
            private const val LEGACY_FLUSH_CHUNK_HEADER: Byte = 2
            private const val PROTOBUF_HEADER: Byte = 3

            fun parse(buffer: ByteBuffer) =
                when (buffer.get(0)) {
                    TX_HEADER -> Tx(buffer.duplicate())
                    LEGACY_FLUSH_CHUNK_HEADER -> FlushChunk(buffer.getLong(1))
                    PROTOBUF_HEADER -> ProtobufMessage.parse(buffer.duplicate().position(1))

                    else -> throw IllegalArgumentException("Unknown message type: ${buffer.get()}")
                }
        }

        class Tx(val payload: ByteBuffer) : Message {
            override fun encode(): ByteBuffer = payload.duplicate()
        }

        sealed class ProtobufMessage : Message {
            abstract fun toLogMessage(): LogMessage

            final override fun encode(): ByteBuffer =
                toLogMessage().let {
                    ByteBuffer.allocate(1 + it.serializedSize).apply {
                        put(PROTOBUF_HEADER)
                        put(it.toByteArray())
                        flip()
                    }
                }

            companion object {
                fun parse(buffer: ByteBuffer): ProtobufMessage =
                    LogMessage.parseFrom(buffer.duplicate().position(1))
                        .let {
                            when (val msgCase = it.messageCase) {
                                MessageCase.FLUSH_CHUNK -> FlushChunk(it.flushChunk.expectedChunkTxId)
                                MessageCase.TRIES_ADDED -> TriesAdded(it.triesAdded.triesList)
                                else -> throw IllegalArgumentException("Unknown protobuf message type: $msgCase")
                            }
                        }
            }
        }

        data class FlushChunk(val expectedChunkTxId: LogOffset) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                flushChunk = flushChunk { expectedChunkTxId = this@FlushChunk.expectedChunkTxId }
            }
        }

        data class TriesAdded(val tries: List<AddedTrie>) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                triesAdded = triesAdded { tries.addAll(this@TriesAdded.tries) }
            }
        }
    }

    interface Factory {
        fun openLog(): Log
    }

    /*
     * We read this once from the existing log at startup,
     * so that if we're starting up a new node it catches up to the latest offset,
     * then it's the latest-submitted-offset of _this_ node.
     */
    val latestSubmittedOffset: LogOffset

    fun appendMessage(message: Message): CompletableFuture<LogOffset>

    fun subscribe(subscriber: Subscriber) : Subscription

    @FunctionalInterface
    fun interface Subscription : AutoCloseable

    class Record(
        val logOffset: LogOffset,
        val logTimestamp: Instant,
        val message: Message
    )

    interface Subscriber {
        val latestCompletedOffset: LogOffset
        fun processRecords(records: List<Record>)
    }
}
