package xtdb.api.log

import xtdb.database.Database
import xtdb.database.DatabaseName
import xtdb.log.proto.SourceLogMessage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.attachDatabase
import xtdb.log.proto.blockUploaded
import xtdb.log.proto.detachDatabase
import xtdb.log.proto.flushBlock
import xtdb.log.proto.sourceLogMessage
import xtdb.log.proto.triesAdded
import xtdb.storage.StorageEpoch
import xtdb.trie.BlockIndex
import java.nio.ByteBuffer

sealed interface SourceMessage {

    fun encode(): ByteArray

    companion object Codec : MessageCodec<SourceMessage> {
        private const val TX_HEADER: Byte = -1
        private const val LEGACY_FLUSH_BLOCK_HEADER: Byte = 2
        private const val PROTOBUF_HEADER: Byte = 3

        override fun encode(message: SourceMessage): ByteArray = message.encode()

        override fun decode(bytes: ByteArray): SourceMessage? = parse(bytes)

        @JvmStatic
        fun parse(bytes: ByteArray) =
            when (bytes[0]) {
                TX_HEADER -> Tx(bytes)
                LEGACY_FLUSH_BLOCK_HEADER -> null
                PROTOBUF_HEADER -> ProtobufMessage.parse(ByteBuffer.wrap(bytes).position(1))

                else -> throw IllegalArgumentException("Unknown message type: ${bytes[0]}")
            }
    }

    class Tx(val payload: ByteArray) : SourceMessage {
        override fun encode(): ByteArray = payload
    }

    sealed class ProtobufMessage : SourceMessage {
        abstract fun toLogMessage(): SourceLogMessage

        final override fun encode(): ByteArray =
            toLogMessage().let {
                ByteBuffer.allocate(1 + it.serializedSize).apply {
                    put(PROTOBUF_HEADER)
                    put(it.toByteArray())
                    flip()
                }.array()
            }

        companion object {
            fun parse(buffer: ByteBuffer): ProtobufMessage? =
                SourceLogMessage.parseFrom(buffer.duplicate().position(1))
                    .let { msg ->
                        when (msg.messageCase) {
                            SourceLogMessage.MessageCase.FLUSH_BLOCK ->
                                msg.flushBlock
                                    .takeIf { it.hasExpectedBlockIdx() }
                                    ?.expectedBlockIdx
                                    ?.let { FlushBlock(it) }

                            SourceLogMessage.MessageCase.TRIES_ADDED -> msg.triesAdded.let {
                                TriesAdded(it.storageVersion, it.storageEpoch, it.triesList)
                            }

                            SourceLogMessage.MessageCase.ATTACH_DATABASE -> msg.attachDatabase.let {
                                AttachDatabase(it.dbName, Database.Config.fromProto(it.config))
                            }

                            SourceLogMessage.MessageCase.DETACH_DATABASE -> DetachDatabase(msg.detachDatabase.dbName)

                            SourceLogMessage.MessageCase.BLOCK_UPLOADED -> msg.blockUploaded.let {
                                BlockUploaded(it.storageVersion, it.storageEpoch, it.blockIndex, it.latestProcessedMsgId, it.triesList)
                            }

                            else -> null
                        }
                    }
        }
    }

    data class FlushBlock(val expectedBlockIdx: BlockIndex?) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            flushBlock = flushBlock { this@FlushBlock.expectedBlockIdx?.let { expectedBlockIdx = it } }
        }
    }

    data class TriesAdded(
        val storageVersion: Int, val storageEpoch: StorageEpoch, val tries: List<TrieDetails>
    ) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            triesAdded = triesAdded {
                storageVersion = this@TriesAdded.storageVersion
                storageEpoch = this@TriesAdded.storageEpoch
                tries.addAll(this@TriesAdded.tries)
            }
        }
    }

    data class AttachDatabase(val dbName: DatabaseName, val config: Database.Config) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            attachDatabase = attachDatabase {
                this.dbName = this@AttachDatabase.dbName
                this.config = this@AttachDatabase.config.serializedConfig
            }
        }
    }

    data class DetachDatabase(val dbName: DatabaseName) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            detachDatabase = detachDatabase {
                this.dbName = this@DetachDatabase.dbName
            }
        }
    }

    data class BlockUploaded(
        val storageVersion: Int, val storageEpoch: StorageEpoch,
        val blockIndex: BlockIndex, val latestProcessedMsgId: MessageId,
        val tries: List<TrieDetails>
    ) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            blockUploaded = blockUploaded {
                this.storageVersion = this@BlockUploaded.storageVersion
                this.storageEpoch = this@BlockUploaded.storageEpoch
                this.blockIndex = this@BlockUploaded.blockIndex
                this.latestProcessedMsgId = this@BlockUploaded.latestProcessedMsgId
                tries.addAll(this@BlockUploaded.tries)
            }
        }
    }
}
