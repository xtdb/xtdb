package xtdb.api.log

import com.google.protobuf.ByteString
import xtdb.database.Database
import xtdb.log.proto.ReplicaLogMessage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.attachDatabase
import xtdb.log.proto.blockBoundary
import xtdb.log.proto.blockUploaded
import xtdb.log.proto.detachDatabase
import xtdb.log.proto.noOp
import xtdb.log.proto.replicaLogMessage
import xtdb.log.proto.resolvedTx
import xtdb.log.proto.triesAdded
import xtdb.storage.StorageEpoch
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.trie.BlockIndex
import xtdb.util.TransitFormat.MSGPACK
import xtdb.util.readTransit
import xtdb.util.writeTransit
import java.nio.ByteBuffer
import java.time.Instant

sealed interface ReplicaMessage {

    fun encode(): ByteArray

    companion object Codec : MessageCodec<ReplicaMessage> {
        private const val PROTOBUF_HEADER: Byte = 3

        override fun encode(message: ReplicaMessage): ByteArray = message.encode()

        override fun decode(bytes: ByteArray): ReplicaMessage? = parse(ByteBuffer.wrap(bytes).position(1))

        fun parse(buffer: ByteBuffer): ReplicaMessage? =
            ReplicaLogMessage.parseFrom(buffer.duplicate().position(1))
                .let { msg ->
                    when (msg.messageCase) {
                        ReplicaLogMessage.MessageCase.RESOLVED_TX -> msg.resolvedTx.let {
                            val dbOp = when (it.dbOpCase) {
                                xtdb.log.proto.ResolvedTx.DbOpCase.ATTACH_DATABASE ->
                                    it.attachDatabase.let { a -> DbOp.Attach(a.dbName, Database.Config.fromProto(a.config)) }
                                xtdb.log.proto.ResolvedTx.DbOpCase.DETACH_DATABASE ->
                                    DbOp.Detach(it.detachDatabase.dbName)
                                else -> null
                            }
                            ResolvedTx(
                                it.txId,
                                fromMicros(it.systemTimeMicros),
                                it.committed,
                                it.error.toByteArray().let { bs ->
                                    if (bs.isEmpty()) null else readTransit(bs, MSGPACK) as Throwable
                                },
                                it.tableDataMap.mapValues { (_, v) -> v.toByteArray() },
                                dbOp
                            )
                        }

                        ReplicaLogMessage.MessageCase.TRIES_ADDED -> msg.triesAdded.let {
                            TriesAdded(it.storageVersion, it.storageEpoch, it.triesList)
                        }

                        ReplicaLogMessage.MessageCase.BLOCK_BOUNDARY -> msg.blockBoundary.let {
                            BlockBoundary(it.blockIndex, it.latestProcessedMsgId)
                        }

                        ReplicaLogMessage.MessageCase.BLOCK_UPLOADED -> msg.blockUploaded.let {
                            BlockUploaded(it.storageVersion, it.storageEpoch, it.blockIndex, it.latestProcessedMsgId, it.triesList)
                        }

                        ReplicaLogMessage.MessageCase.NO_OP -> NoOp

                        else -> null
                    }
                }
    }

    sealed class ProtobufMessage : ReplicaMessage {
        abstract fun toLogMessage(): ReplicaLogMessage

        final override fun encode(): ByteArray =
            toLogMessage().let {
                ByteBuffer.allocate(1 + it.serializedSize).apply {
                    put(PROTOBUF_HEADER)
                    put(it.toByteArray())
                    flip()
                }.array()
            }
    }

    data class ResolvedTx(
        val txId: MessageId,
        val systemTime: Instant,
        val committed: Boolean,
        val error: Throwable?,
        val tableData: Map<String, ByteArray>,
        val dbOp: DbOp? = null
    ) : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage {
            resolvedTx = resolvedTx {
                this.txId = this@ResolvedTx.txId
                this.systemTimeMicros = this@ResolvedTx.systemTime.asMicros
                this.committed = this@ResolvedTx.committed
                this.error = this@ResolvedTx.error?.let { ByteString.copyFrom(writeTransit(it, MSGPACK)) } ?: ByteString.EMPTY
                this@ResolvedTx.tableData.forEach { (k, v) ->
                    this.tableData[k] = ByteString.copyFrom(v)
                }
                when (val op = this@ResolvedTx.dbOp) {
                    is DbOp.Attach -> attachDatabase = attachDatabase {
                        this.dbName = op.dbName
                        this.config = op.config.serializedConfig
                    }

                    is DbOp.Detach -> detachDatabase = detachDatabase {
                        this.dbName = op.dbName
                    }

                    null -> {}
                }
            }
        }
    }

    data class TriesAdded(
        val storageVersion: Int, val storageEpoch: StorageEpoch, val tries: List<TrieDetails>
    ) : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage {
            triesAdded = triesAdded {
                storageVersion = this@TriesAdded.storageVersion
                storageEpoch = this@TriesAdded.storageEpoch
                tries.addAll(this@TriesAdded.tries)
            }
        }
    }

    data class BlockBoundary(val blockIndex: BlockIndex, val latestProcessedMsgId: MessageId) : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage {
            blockBoundary = blockBoundary {
                this.blockIndex = this@BlockBoundary.blockIndex
                this.latestProcessedMsgId = this@BlockBoundary.latestProcessedMsgId
            }
        }
    }

    data class BlockUploaded(
        val storageVersion: Int, val storageEpoch: StorageEpoch,
        val blockIndex: BlockIndex, val latestProcessedMsgId: MessageId,
        val tries: List<TrieDetails>
    ) : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage {
            blockUploaded = blockUploaded {
                this.storageVersion = this@BlockUploaded.storageVersion
                this.storageEpoch = this@BlockUploaded.storageEpoch
                this.blockIndex = this@BlockUploaded.blockIndex
                this.latestProcessedMsgId = this@BlockUploaded.latestProcessedMsgId
                tries.addAll(this@BlockUploaded.tries)
            }
        }
    }

    data object NoOp : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage { noOp = noOp {} }
    }
}
