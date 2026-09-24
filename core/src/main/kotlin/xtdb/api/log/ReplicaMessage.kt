package xtdb.api.log

import com.google.protobuf.ByteString
import xtdb.api.tx.ExternalSourceToken
import xtdb.database.Database
import xtdb.types.MessageId
import xtdb.log.proto.ReplicaLogMessage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.attachDatabase
import xtdb.log.proto.blockBoundary
import xtdb.log.proto.blockUploaded
import xtdb.log.proto.detachDatabase
import xtdb.log.proto.noOp
import xtdb.log.proto.oversizedMessage
import xtdb.log.proto.replicaLogMessage
import xtdb.log.proto.resolvedTx
import xtdb.log.proto.triesAdded
import xtdb.log.proto.triesDeleted
import xtdb.storage.StorageEpoch
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.trie.BlockIndex
import xtdb.trie.TrieKey
import xtdb.util.StringUtil.asLexHex
import xtdb.util.StringUtil.fromLexHex
import xtdb.util.TransitFormat.MSGPACK
import xtdb.util.asPath
import xtdb.util.readTransit
import xtdb.util.writeTransit
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Instant

sealed interface ReplicaMessage {

    // The leader term that produced this message; used for read-side fencing. 0 only on a record written
    // before terms existed, where proto3's scalar default supplies it. See #5817.
    val termId: Long

    // This message's position within its term: 0 for the claim that opens it, then consecutive from 1 for
    // what its leader writes. Null on a record written before positions existed. See #6105.
    val termSeq: Long?

    /** This message at [termSeq] — stamped by the term's append pump, which alone knows the position. */
    fun withTermSeq(termSeq: Long): ReplicaMessage

    fun encode(): ByteArray

    companion object Codec : MessageCodec<ReplicaMessage> {
        private const val PROTOBUF_HEADER: Byte = 3

        override fun encode(message: ReplicaMessage): ByteArray = message.encode()

        override fun decode(bytes: ByteArray): ReplicaMessage? = parse(ByteBuffer.wrap(bytes).position(1))

        fun parse(buffer: ByteBuffer): ReplicaMessage? =
            ReplicaLogMessage.parseFrom(buffer.duplicate().position(1))
                .let { msg ->
                    val termSeq = if (msg.hasTermSeq()) msg.termSeq else null

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
                                it.tableDataMap,
                                dbOp,
                                it.externalSourceToken.takeIf { _ -> it.hasExternalSourceToken() }?.toByteArray(),
                                if (it.hasSrcMsgId()) it.srcMsgId else null,
                                termId = msg.termId, termSeq = termSeq,
                            )
                        }

                        ReplicaLogMessage.MessageCase.TRIES_ADDED -> msg.triesAdded.let {
                            TriesAdded(it.storageVersion, it.storageEpoch, it.triesList, it.sourceMsgId, termId = msg.termId, termSeq = termSeq)
                        }

                        ReplicaLogMessage.MessageCase.BLOCK_BOUNDARY -> msg.blockBoundary.let {
                            BlockBoundary(
                                it.blockIndex, it.latestProcessedMsgId,
                                it.externalSourceToken.takeIf { _ -> it.hasExternalSourceToken() }?.toByteArray(),
                                termId = msg.termId, termSeq = termSeq,
                            )
                        }

                        ReplicaLogMessage.MessageCase.BLOCK_UPLOADED -> msg.blockUploaded.let {
                            BlockUploaded(
                                it.storageVersion, it.storageEpoch, it.blockIndex, it.latestProcessedMsgId, it.triesList,
                                it.externalSourceToken.takeIf { _ -> it.hasExternalSourceToken() }?.toByteArray(),
                                termId = msg.termId, termSeq = termSeq,
                            )
                        }

                        ReplicaLogMessage.MessageCase.NO_OP -> msg.noOp.let {
                            NoOp(if (it.hasSrcMsgId()) it.srcMsgId else null, termId = msg.termId, termSeq = termSeq)
                        }

                        ReplicaLogMessage.MessageCase.TRIES_DELETED -> msg.triesDeleted.let {
                            TriesDeleted(it.tableName, it.trieKeysList.toSet(), termId = msg.termId, termSeq = termSeq)
                        }

                        ReplicaLogMessage.MessageCase.OVERSIZED_MESSAGE -> msg.oversizedMessage.let {
                            OversizedMessage(
                                it.storageVersion, it.storageEpoch, it.blockIndex, it.payloadId,
                                termId = msg.termId, termSeq = termSeq,
                            )
                        }

                        else -> null
                    }
                }
    }

    sealed class ProtobufMessage : ReplicaMessage {
        abstract fun toLogMessage(): ReplicaLogMessage

        final override fun encode(): ByteArray =
            toLogMessage().toBuilder().setTermId(termId).apply { this@ProtobufMessage.termSeq?.let { setTermSeq(it) } }.build()
                .toHeaderedBytes(PROTOBUF_HEADER)
    }

    data class ResolvedTx(
        val txId: MessageId,
        val systemTime: Instant,
        val committed: Boolean,
        val error: Throwable?,
        val tableData: Map<String, ByteString>,
        val dbOp: DbOp? = null,
        val externalSourceToken: ExternalSourceToken? = null,
        // The source-log watermark when this record was produced: for a source-log tx, its own
        // msgId; for an ext-source tx, the leader's current source-log position (so followers keep
        // their `latestSourceMsgId` in step between block boundaries). Null only on legacy records
        // written before this field existed (see #5586).
        val srcMsgId: MessageId? = null,
        override val termId: Long,
        override val termSeq: Long? = null,
    ) : ProtobufMessage() {
        override fun withTermSeq(termSeq: Long) = copy(termSeq = termSeq)

        override fun toLogMessage() = replicaLogMessage {
            resolvedTx = resolvedTx {
                this.txId = this@ResolvedTx.txId
                this.systemTimeMicros = this@ResolvedTx.systemTime.asMicros
                this.committed = this@ResolvedTx.committed
                this.error = this@ResolvedTx.error?.let { ByteString.copyFrom(writeTransit(it, MSGPACK)) } ?: ByteString.EMPTY
                this.tableData.putAll(this@ResolvedTx.tableData)
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
                this@ResolvedTx.externalSourceToken?.let { externalSourceToken = ByteString.copyFrom(it) }
                this@ResolvedTx.srcMsgId?.let { srcMsgId = it }
            }
        }
    }

    data class TriesAdded(
        val storageVersion: Int, val storageEpoch: StorageEpoch, val tries: List<TrieDetails>,
        val sourceMsgId: MessageId,
        override val termId: Long,
        override val termSeq: Long? = null,
    ) : ProtobufMessage() {
        override fun withTermSeq(termSeq: Long) = copy(termSeq = termSeq)

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
        val externalSourceToken: ExternalSourceToken? = null,
        override val termId: Long,
        override val termSeq: Long? = null,
    ) : ProtobufMessage() {
        override fun withTermSeq(termSeq: Long) = copy(termSeq = termSeq)

        override fun toLogMessage() = replicaLogMessage {
            blockBoundary = blockBoundary {
                this.blockIndex = this@BlockBoundary.blockIndex
                this.latestProcessedMsgId = this@BlockBoundary.latestProcessedMsgId
                this@BlockBoundary.externalSourceToken?.let { this.externalSourceToken = ByteString.copyFrom(it) }
            }
        }
    }

    data class BlockUploaded(
        val storageVersion: Int, val storageEpoch: StorageEpoch,
        val blockIndex: BlockIndex, val latestProcessedMsgId: MessageId,
        val tries: List<TrieDetails>,
        val externalSourceToken: ExternalSourceToken? = null,
        override val termId: Long,
        override val termSeq: Long? = null,
    ) : ProtobufMessage() {
        override fun withTermSeq(termSeq: Long) = copy(termSeq = termSeq)

        override fun toLogMessage() = replicaLogMessage {
            blockUploaded = blockUploaded {
                this.storageVersion = this@BlockUploaded.storageVersion
                this.storageEpoch = this@BlockUploaded.storageEpoch
                this.blockIndex = this@BlockUploaded.blockIndex
                this.latestProcessedMsgId = this@BlockUploaded.latestProcessedMsgId
                tries.addAll(this@BlockUploaded.tries)
                this@BlockUploaded.externalSourceToken?.let { this.externalSourceToken = ByteString.copyFrom(it) }
            }
        }
    }

    // `srcMsgId` carries the leader's source-log watermark when no other record propagates it
    // (a FlushBlock that finishes no block); followers advance `latestSourceMsgId` on it. Null
    // when used purely as a transition replay-target marker.
    data class NoOp(
        val srcMsgId: MessageId? = null,
        override val termId: Long,
        override val termSeq: Long? = null,
    ) : ProtobufMessage() {
        override fun withTermSeq(termSeq: Long) = copy(termSeq = termSeq)

        override fun toLogMessage() = replicaLogMessage {
            noOp = noOp { this@NoOp.srcMsgId?.let { srcMsgId = it } }
        }
    }

    data class TriesDeleted(
        val tableName: String,
        val trieKeys: Set<TrieKey>,
        override val termId: Long,
        override val termSeq: Long? = null,
    ) : ProtobufMessage() {
        override fun withTermSeq(termSeq: Long) = copy(termSeq = termSeq)

        override fun toLogMessage() = replicaLogMessage {
            triesDeleted = triesDeleted {
                tableName = this@TriesDeleted.tableName
                trieKeys.addAll(this@TriesDeleted.trieKeys)
            }
        }
    }

    /**
     * Stands in for a message the log declined for its size; [path] holds that message's own encoding.
     *
     * [termId] is the wrapped message's, so a reader fences on this envelope without fetching the payload.
     * [blockIndex] is the open block as the writer last saw it, rather than the last completed one, so that
     * GC — which collects on it — errs towards keeping a payload rather than dropping one still referenced.
     * It is read off the catalog without synchronising against the apply coroutine that advances it, so a
     * block adopted in between leaves the payload keyed one block early; `blocksToKeep` is the margin.
     *
     * Writer, reader and GC all go through [path] and [blockIndexOf]; the key format lives nowhere else.
     */
    data class OversizedMessage(
        val storageVersion: Int, val storageEpoch: StorageEpoch,
        val blockIndex: BlockIndex, val payloadId: String,
        override val termId: Long,
        override val termSeq: Long? = null,
    ) : ProtobufMessage() {
        override fun withTermSeq(termSeq: Long) = copy(termSeq = termSeq)

        val path: Path get() = oversizedDir.resolve("b${blockIndex.asLexHex}-$payloadId.binpb")

        override fun toLogMessage() = replicaLogMessage {
            oversizedMessage = oversizedMessage {
                this.storageVersion = this@OversizedMessage.storageVersion
                this.storageEpoch = this@OversizedMessage.storageEpoch
                this.blockIndex = this@OversizedMessage.blockIndex
                this.payloadId = this@OversizedMessage.payloadId
            }
        }

        companion object {
            val oversizedDir = "oversized".asPath

            private val PAYLOAD_KEY = Regex("b(\\p{XDigit}+)-.+\\.binpb")

            /** The block [path] was written under, or null where it isn't an offloaded payload's key. */
            fun blockIndexOf(path: Path): BlockIndex? =
                PAYLOAD_KEY.matchEntire(path.fileName.toString())?.groups?.get(1)?.value?.fromLexHex
        }
    }
}
