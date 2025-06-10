@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.log.proto.*
import xtdb.log.proto.LogMessage.MessageCase
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.CompletableFuture

typealias LogOffset = Long
typealias LogTimestamp = Instant
typealias MessageId = Long

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
            private const val LEGACY_FLUSH_BLOCK_HEADER: Byte = 2
            private const val PROTOBUF_HEADER: Byte = 3

            fun parse(buffer: ByteBuffer) =
                when (buffer.get(0)) {
                    TX_HEADER -> Tx(buffer.duplicate())
                    LEGACY_FLUSH_BLOCK_HEADER -> FlushBlock(buffer.getLong(1))
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
                                MessageCase.FLUSH_BLOCK -> FlushBlock(it.flushBlock.expectedBlockTxId)
                                MessageCase.TRIES_ADDED ->
                                    TriesAdded(it.triesAdded.storageVersion, it.triesAdded.triesList)

                                else -> throw IllegalArgumentException("Unknown protobuf message type: $msgCase")
                            }
                        }
            }
        }

        data class FlushBlock(val expectedBlockTxId: MessageId) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                flushBlock = flushBlock { expectedBlockTxId = this@FlushBlock.expectedBlockTxId }
            }
        }

        data class TriesAdded(val storageVersion: Int, val tries: List<TrieDetails>) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                triesAdded = triesAdded {
                    storageVersion = this@TriesAdded.storageVersion
                    tries.addAll(this@TriesAdded.tries)
                }
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

    val epoch: Int

    class MessageMetadata(
        val logOffset: LogOffset,
        val logTimestamp: LogTimestamp
    )

    fun appendMessage(message: Message): CompletableFuture<MessageMetadata>

    fun subscribe(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription

    @FunctionalInterface
    fun interface Subscription : AutoCloseable

    class Record(
        val logOffset: LogOffset,
        val logTimestamp: Instant,
        val message: Message
    )

    interface Subscriber {
        val latestProcessedMsgId: MessageId
        val latestSubmittedMsgId: MessageId
        fun processRecords(records: List<Record>)
    }
}
