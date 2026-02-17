@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)

package xtdb.api.log

import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.database.Database
import xtdb.database.DatabaseName
import xtdb.database.proto.DatabaseConfig
import xtdb.database.proto.DatabaseConfig.LogCase.*
import xtdb.log.proto.*
import xtdb.log.proto.LogMessage.MessageCase
import xtdb.storage.StorageEpoch
import xtdb.trie.BlockIndex
import xtdb.util.MsgIdUtil
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture
import com.google.protobuf.Any as ProtoAny


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

        fun encode(): ByteArray

        companion object {
            private const val TX_HEADER: Byte = -1
            private const val LEGACY_FLUSH_BLOCK_HEADER: Byte = 2
            private const val PROTOBUF_HEADER: Byte = 3

            @JvmStatic
            fun parse(bytes: ByteArray) =
                when (bytes[0]) {
                    TX_HEADER -> Tx(bytes)
                    LEGACY_FLUSH_BLOCK_HEADER -> null
                    PROTOBUF_HEADER -> ProtobufMessage.parse(ByteBuffer.wrap(bytes).position(1))

                    else -> throw IllegalArgumentException("Unknown message type: ${bytes[0]}")
                }
        }

        class Tx(val payload: ByteArray) : Message {
            override fun encode(): ByteArray = payload
        }

        sealed class ProtobufMessage : Message {
            abstract fun toLogMessage(): LogMessage

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
                    LogMessage.parseFrom(buffer.duplicate().position(1))
                        .let { msg ->
                            when (msg.messageCase) {
                                MessageCase.FLUSH_BLOCK ->
                                    msg.flushBlock
                                        .takeIf { it.hasExpectedBlockIdx() }
                                        ?.expectedBlockIdx
                                        ?.let { FlushBlock(it) }

                                MessageCase.TRIES_ADDED -> msg.triesAdded.let {
                                    TriesAdded(it.storageVersion, it.storageEpoch, it.triesList)
                                }

                                MessageCase.ATTACH_DATABASE -> msg.attachDatabase.let {
                                    AttachDatabase(it.dbName, Database.Config.fromProto(it.config))
                                }

                                MessageCase.DETACH_DATABASE -> DetachDatabase(msg.detachDatabase.dbName)

                                MessageCase.BLOCK_UPLOADED -> msg.blockUploaded.let {
                                    BlockUploaded(it.blockIndex, it.latestProcessedMsgId, it.storageEpoch)
                                }

                                MessageCase.RESOLVED_TX -> msg.resolvedTx.let {
                                    ResolvedTx(
                                        it.txId, it.systemTimeMicros, it.committed,
                                        it.error.toByteArray(),
                                        it.tableDataMap.mapValues { (_, v) -> v.toByteArray() }
                                    )
                                }

                                else -> null
                            }
                        }
            }
        }

        data class FlushBlock(val expectedBlockIdx: BlockIndex?) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                flushBlock = flushBlock { this@FlushBlock.expectedBlockIdx?.let { expectedBlockIdx = it } }
            }
        }

        data class TriesAdded(
            val storageVersion: Int, val storageEpoch: StorageEpoch, val tries: List<TrieDetails>
        ) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                triesAdded = triesAdded {
                    storageVersion = this@TriesAdded.storageVersion
                    storageEpoch = this@TriesAdded.storageEpoch
                    tries.addAll(this@TriesAdded.tries)
                }
            }
        }

        data class AttachDatabase(val dbName: DatabaseName, val config: Database.Config) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                attachDatabase = attachDatabase {
                    this.dbName = this@AttachDatabase.dbName
                    this.config = this@AttachDatabase.config.serializedConfig
                }
            }
        }

        data class DetachDatabase(val dbName: DatabaseName) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                detachDatabase = detachDatabase {
                    this.dbName = this@DetachDatabase.dbName
                }
            }
        }

        data class BlockUploaded(val blockIndex: BlockIndex, val latestProcessedMsgId: MessageId, val storageEpoch: StorageEpoch) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                blockUploaded = blockUploaded {
                    this.blockIndex = this@BlockUploaded.blockIndex
                    this.latestProcessedMsgId = this@BlockUploaded.latestProcessedMsgId
                    this.storageEpoch = this@BlockUploaded.storageEpoch
                }
            }
        }

        data class ResolvedTx(
            val txId: MessageId,
            val systemTimeMicros: Long,
            val committed: Boolean,
            val error: ByteArray,
            val tableData: Map<String, ByteArray>
        ) : ProtobufMessage() {
            override fun toLogMessage() = logMessage {
                resolvedTx = resolvedTx {
                    this.txId = this@ResolvedTx.txId
                    this.systemTimeMicros = this@ResolvedTx.systemTimeMicros
                    this.committed = this@ResolvedTx.committed
                    this.error = com.google.protobuf.ByteString.copyFrom(this@ResolvedTx.error)
                    this@ResolvedTx.tableData.forEach { (k, v) ->
                        this.tableData[k] = com.google.protobuf.ByteString.copyFrom(v)
                    }
                }
            }
        }
    }

    interface Factory {
        fun openLog(clusters: Map<LogClusterAlias, Cluster>): Log
        fun openReadOnlyLog(clusters: Map<LogClusterAlias, Cluster>): Log

        fun writeTo(dbConfig: DatabaseConfig.Builder)

        companion object {
            private val otherLogs = ServiceLoader.load(Registration::class.java).associateBy { it.protoTag }

            val serializersModule = SerializersModule {
                polymorphic(Factory::class) {
                    subclass(InMemoryLog.Factory::class)
                    subclass(LocalLog.Factory::class)

                    for (reg in ServiceLoader.load(Registration::class.java))
                        reg.registerSerde(this)
                }
            }

            internal fun fromProto(config: DatabaseConfig): Factory =
                when (config.logCase) {
                    IN_MEMORY_LOG -> inMemoryLog
                    LOCAL_LOG -> localLog(config.localLog.path.asPath)
                    OTHER_LOG -> config.otherLog.let {
                        (otherLogs[it.typeUrl] ?: error("unknown log")).fromProto(it)
                    }

                    else -> error("invalid log: ${config.logCase}")
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
        val protoTag: String
        fun fromProto(msg: ProtoAny): Factory
    }

    /*
     * We read this once from the existing log at startup,
     * so that if we're starting up a new node it catches up to the latest offset,
     * then it's the latest-submitted-offset of _this_ node.
     */
    val latestSubmittedOffset: LogOffset

    val epoch: Int

    val latestSubmittedMsgId: MessageId
        get() = MsgIdUtil.offsetToMsgId(epoch, latestSubmittedOffset)

    class MessageMetadata(
        val logOffset: LogOffset,
        val logTimestamp: LogTimestamp
    )

    fun appendMessage(message: Message): CompletableFuture<MessageMetadata>

    /**
     * @param transactionalId uniquely identifies this producer for Kafka's transaction coordinator.
     *   Must be stable across restarts for transaction recovery.
     */
    fun openAtomicProducer(transactionalId: String): AtomicProducer

    interface AtomicProducer : AutoCloseable {
        fun openTx(): Tx

        interface Tx : AutoCloseable {
            fun appendMessage(message: Message): CompletableFuture<MessageMetadata>
            fun commit()
            fun abort()
        }
    }

    fun readLastMessage(): Message?

    fun tailAll(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription

    interface GroupSubscriber : Subscriber {
        /**
         * @return Map of partition to next offset to consume from.
         *         Partitions not in the map use Kafka's default (committed offset or auto.offset.reset).
         */
        fun onPartitionsAssigned(partitions: Collection<Int>): Map<Int, LogOffset>

        fun onPartitionsRevoked(partitions: Collection<Int>)

        fun onPartitionsLost(partitions: Collection<Int>) = onPartitionsRevoked(partitions)
    }

    fun subscribe(subscriber: GroupSubscriber): Subscription

    @FunctionalInterface
    fun interface Subscription : AutoCloseable

    class Record(
        val logOffset: LogOffset,
        val logTimestamp: Instant,
        val message: Message
    )

    interface Subscriber {
        fun processRecords(records: List<Record>)
    }
}
