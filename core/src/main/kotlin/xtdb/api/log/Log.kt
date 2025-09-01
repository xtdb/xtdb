@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.log.proto.*
import xtdb.log.proto.LogMessage.MessageCase
import xtdb.trie.BlockIndex
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture

typealias LogOffset = Long
typealias LogTimestamp = Instant
typealias MessageId = Long

typealias LogClusterAlias = String

interface Log : AutoCloseable {

    interface Cluster : AutoCloseable {

        interface Factory<C : Cluster> {
            fun open(): C

            companion object {
                val serializersModule = SerializersModule {
                    polymorphic(Factory::class) {
                        for (reg in ServiceLoader.load(Registration::class.java))
                            reg.registerSerde(this)
                    }
                }
            }
        }

        interface Registration {
            fun registerSerde(builder: PolymorphicModuleBuilder<Factory<*>>)
        }
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
                    LEGACY_FLUSH_BLOCK_HEADER -> null
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
                fun parse(buffer: ByteBuffer): ProtobufMessage? =
                    LogMessage.parseFrom(buffer.duplicate().position(1))
                        .let { msg ->
                            when (val msgCase = msg.messageCase) {
                                MessageCase.FLUSH_BLOCK ->
                                    msg.flushBlock
                                        .takeIf { it.hasExpectedBlockIdx() }
                                        ?.expectedBlockIdx
                                        ?.let { FlushBlock(it) }

                                MessageCase.TRIES_ADDED ->
                                    TriesAdded(msg.triesAdded.storageVersion, msg.triesAdded.triesList)

                                else -> throw IllegalArgumentException("Unknown protobuf message type: $msgCase")
                            }
                        }
            }
        }

        data class FlushBlock(val expectedBlockIdx: BlockIndex?) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                flushBlock = flushBlock { this@FlushBlock.expectedBlockIdx?.let { expectedBlockIdx = it } }
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
        fun openLog(clusters: Map<LogClusterAlias, Cluster>): Log

        companion object {
            val serializersModule = SerializersModule {
                polymorphic(Factory::class) {
                    subclass(InMemoryLog.Factory::class)
                    subclass(LocalLog.Factory::class)

                    for (reg in ServiceLoader.load(Registration::class.java))
                        reg.registerSerde(this)
                }
            }
        }
    }

    companion object {
        @JvmStatic
        val inMemoryLog get() = InMemoryLog.Factory()

        @JvmStatic
        fun localLog(rootPath: Path) = LocalLog.Factory(rootPath)

        @Suppress("unused")
        @JvmSynthetic
        fun localLog(path: Path, configure: LocalLog.Factory.() -> Unit) = localLog(path).also(configure)
    }

    interface Registration {
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
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
