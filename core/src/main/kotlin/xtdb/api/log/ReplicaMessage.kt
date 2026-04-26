package xtdb.api.log

import com.google.protobuf.ByteString
import xtdb.database.ExternalSourceToken
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
import xtdb.log.proto.triesDeleted
import xtdb.storage.StorageEpoch
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.trie.BlockIndex
import xtdb.trie.TrieKey
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
                                dbOp,
                                it.externalSourceToken.takeIf { _ -> it.hasExternalSourceToken() }
                            )
                        }

                        ReplicaLogMessage.MessageCase.TRIES_ADDED -> msg.triesAdded.let {
                            TriesAdded(it.storageVersion, it.storageEpoch, it.triesList, it.sourceMsgId)
                        }

                        ReplicaLogMessage.MessageCase.BLOCK_BOUNDARY -> msg.blockBoundary.let {
                            BlockBoundary(
                                it.blockIndex, it.latestProcessedMsgId,
                                it.externalSourceToken.takeIf { _ -> it.hasExternalSourceToken() }
                            )
                        }

                        ReplicaLogMessage.MessageCase.BLOCK_UPLOADED -> msg.blockUploaded.let {
                            BlockUploaded(
                                it.storageVersion, it.storageEpoch, it.blockIndex, it.latestProcessedMsgId, it.triesList,
                                it.externalSourceToken.takeIf { _ -> it.hasExternalSourceToken() }
                            )
                        }

                        ReplicaLogMessage.MessageCase.NO_OP -> NoOp

                        ReplicaLogMessage.MessageCase.TRIES_DELETED -> msg.triesDeleted.let {
                            TriesDeleted(it.tableName, it.trieKeysList.toSet())
                        }

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
        val dbOp: DbOp? = null,
        val externalSourceToken: ExternalSourceToken? = null,
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
                this@ResolvedTx.externalSourceToken?.let { externalSourceToken = it }
            }
        }
    }

    data class TriesAdded(
        val storageVersion: Int, val storageEpoch: StorageEpoch, val tries: List<TrieDetails>,
        val sourceMsgId: MessageId = 0,
    ) : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage {
            triesAdded = triesAdded {
                storageVersion = this@TriesAdded.storageVersion
                storageEpoch = this@TriesAdded.storageEpoch
                tries.addAll(this@TriesAdded.tries)
                sourceMsgId = this@TriesAdded.sourceMsgId
            }
        }
    }

    data class BlockBoundary(
        val blockIndex: BlockIndex, val latestProcessedMsgId: MessageId,
        val externalSourceToken: ExternalSourceToken? = null
    ) : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage {
            blockBoundary = blockBoundary {
                this.blockIndex = this@BlockBoundary.blockIndex
                this.latestProcessedMsgId = this@BlockBoundary.latestProcessedMsgId
                this@BlockBoundary.externalSourceToken?.let { this.externalSourceToken = it }
            }
        }
    }

    data class BlockUploaded(
        val storageVersion: Int, val storageEpoch: StorageEpoch,
        val blockIndex: BlockIndex, val latestProcessedMsgId: MessageId,
        val tries: List<TrieDetails>,
        val externalSourceToken: ExternalSourceToken? = null
    ) : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage {
            blockUploaded = blockUploaded {
                this.storageVersion = this@BlockUploaded.storageVersion
                this.storageEpoch = this@BlockUploaded.storageEpoch
                this.blockIndex = this@BlockUploaded.blockIndex
                this.latestProcessedMsgId = this@BlockUploaded.latestProcessedMsgId
                tries.addAll(this@BlockUploaded.tries)
                this@BlockUploaded.externalSourceToken?.let { this.externalSourceToken = it }
            }
        }
    }

    data object NoOp : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage { noOp = noOp {} }
    }

    data class TriesDeleted(
        val tableName: String,
        val trieKeys: Set<TrieKey>,
    ) : ProtobufMessage() {
        override fun toLogMessage() = replicaLogMessage {
            triesDeleted = triesDeleted {
                tableName = this@TriesDeleted.tableName
                trieKeys.addAll(this@TriesDeleted.trieKeys)
            }
        }
    }
}
