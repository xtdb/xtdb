@file:UseSerializers(DurationSerde::class, PathSerde::class)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathSerde
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log.*
import xtdb.database.proto.DatabaseConfig
import xtdb.util.MsgIdUtil
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.rethrowingChannelInterrupts
import xtdb.database.proto.localLog
import xtdb.types.LogOffset
import xtdb.types.MessageId
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import java.io.DataInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.time.Instant
import java.time.InstantSource
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.createParentDirectories
import kotlin.io.path.exists
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.Int.Companion.SIZE_BYTES as INT_BYTES
import kotlin.Long.Companion.SIZE_BYTES as LONG_BYTES

class LocalLog<M> @JvmOverloads constructor(
    private val rootPath: Path,
    private val codec: MessageCodec<M>,
    private val instantSource: InstantSource,
    override val epoch: Int,
    val useInstantSourceForNonTx: Boolean,
    coroutineContext: CoroutineContext = Dispatchers.IO,
    private val baseFileName: String = "LOG",
    val partitions: Int = 1,
) : Log<M> {
    private val scope = CoroutineScope(coroutineContext)
    companion object {
        private fun messageSizeBytes(size: Int) = 1 + INT_BYTES + LONG_BYTES + size + LONG_BYTES

        private const val RECORD_SEPARATOR = 0x1E.toByte()

        private fun readLatestSubmittedOffset(logFilePath: Path): LogOffset {
            if (!logFilePath.exists()) return -1

            return FileChannel.open(logFilePath).use { ch ->
                val chSize = ch.size()

                if (chSize == 0L) return -1

                try {
                    val buf = ByteBuffer.allocateDirect(LONG_BYTES)

                    check(ch.read(buf, chSize - LONG_BYTES) == LONG_BYTES) {
                        "Failed to read last offset in log file"
                    }

                    buf.flip().getLong()
                        .also { offset ->
                            check(offset in 0..<chSize) { "Invalid offset in log file: $offset" }
                            ch.position(offset)
                            DataInputStream(Channels.newInputStream(ch)).use { dataStream ->
                                check(dataStream.readByte() == RECORD_SEPARATOR) {
                                    "log file corrupted - expected record separator at $offset"
                                }

                                val size = dataStream.readInt()
                                check(chSize == offset + messageSizeBytes(size)) {
                                    "log file corrupted - record at $offset specifies size $size, but file size is $chSize"
                                }
                            }
                        }
                } catch (e: Exception) {
                    throw IllegalStateException("Failed to read log file", e)
                }
            }
        }

    }

    private fun FileChannel.readMessage(): Record<M>? {
        val pos = position()
        val headerBuf = ByteBuffer.allocateDirect(1 + INT_BYTES + LONG_BYTES)
            .also { read(it); it.flip() }

        check(headerBuf.get() == RECORD_SEPARATOR) { "log file corrupted at $pos - expected record separator" }
        val size = headerBuf.getInt()

        val message =
            codec.decode(ByteBuffer.allocate(size).also { read(it); it.flip() }.array())
                ?: return null

        return Record(epoch, pos, fromMicros(headerBuf.getLong()), message)
            .also { position(pos + messageSizeBytes(size)) }
    }

    internal data class NewMessage<M>(
        val message: M,
        val onCommit: CompletableDeferred<Record<M>>
    )

    // N=1 keeps the pre-#5557 path (byte-identical layout, existing directories/fixtures survive). N>1
    // nests under a role directory. Partition count is immutable post-attach so the two shapes never
    // coexist for the same rootPath.
    private fun fileNameFor(partition: Int): String =
        if (partitions == 1) baseFileName else "$baseFileName/$partition"

    private inner class PartitionState(val partition: Int) {
        val logFilePath: Path = rootPath.resolve(fileNameFor(partition))
        val logFileChannel: FileChannel =
            FileChannel.open(logFilePath.createParentDirectories(), CREATE, WRITE, APPEND)
        val appendCh = Channel<NewMessage<M>>(capacity = 10)
        val committedOffset = MutableStateFlow(readLatestSubmittedOffset(logFilePath))
    }

    private val partitionStates: List<PartitionState> = List(partitions) { PartitionState(it) }

    private fun state(partition: Int): PartitionState =
        partitionStates.getOrNull(partition)
            ?: error("no such partition $partition (partitions=$partitions)")

    private suspend fun PartitionState.writeMessages(msgs: List<NewMessage<M>>): Array<Record<M>> = runInterruptible {
        val initialOffset = logFileChannel.position()

        try {
            val res = Array(msgs.size) { idx ->
                val (msg) = msgs[idx]
                // we only use the instantSource for Tx messages so that the tests
                // that check files can be deterministic
                val ts = if (msg is SourceMessage.Tx || msg is SourceMessage.LegacyTx || useInstantSourceForNonTx) instantSource.instant() else Instant.now()
                val payload = codec.encode(msg)
                val size = payload.size
                val offset = logFileChannel.position()

                logFileChannel.write(
                    ByteBuffer
                        .allocateDirect(messageSizeBytes(size))
                        .run {
                            put(RECORD_SEPARATOR)
                            putInt(size)
                            putLong(ts.asMicros)
                            put(payload)
                            putLong(offset)
                            flip()
                        })

                Record(epoch, offset, ts, msg)
            }

            logFileChannel.force(true)

            res
        } catch (e: ClosedByInterruptException) {
            // Nothing to roll back to: an interrupted channel operation closes the channel as it throws, and
            // this is the partition's one writer, so `truncate` below would only raise ClosedChannelException
            // over the top of the cause. Nothing to roll back *for* either — the only thing that interrupts
            // this writer is the log closing. Re-raised as the interrupt it is, which `runInterruptible`
            // re-casts to cancellation; an IOException would reach the caller as a fault instead.
            throw InterruptedException().apply { initCause(e) }
        } catch (t: Throwable) {
            // Never over the top of the cause that got us here — a failed rollback is worth reporting, but
            // it is the write's failure the caller has to act on.
            try {
                logFileChannel.truncate(initialOffset)
            } catch (e: Throwable) {
                t.addSuppressed(e)
            }

            throw t
        }
    }

    override fun latestSubmittedOffset(partition: Int): LogOffset = state(partition).committedOffset.value

    init {
        for (ps in partitionStates) {
            scope.launch {
                while (true) {
                    val msgs = mutableListOf(ps.appendCh.receive())

                    while (true) {
                        if (msgs.size >= 10) break
                        msgs.add(ps.appendCh.tryReceive().getOrNull() ?: break)
                    }

                    val records = ps.writeMessages(msgs)

                    ps.committedOffset.value = records.last().logOffset
                    msgs.forEachIndexed { idx, msg ->
                        msg.onCommit.complete(records[idx])
                    }
                }
            }
        }
    }

    override suspend fun appendMessage(message: M, partition: Int): MessageMetadata {
        val ps = state(partition)
        return CompletableDeferred<MessageMetadata>()
            .also { res ->
                scope.launch {
                    val onCommit = CompletableDeferred<Record<M>>()
                    ps.appendCh.send(NewMessage(message, onCommit))
                    val record = onCommit.await()
                    res.complete(MessageMetadata(epoch, record.logOffset, record.logTimestamp))
                }
            }
            .await()
    }

    override fun readLastMessage(partition: Int): M? {
        val ps = state(partition)
        if (ps.committedOffset.value < 0) return null

        return FileChannel.open(ps.logFilePath).use { ch ->
            ch.position(ps.committedOffset.value)
            ch.readMessage()?.message
        }
    }

    override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId) = sequence {
        if (MsgIdUtil.msgIdToEpoch(fromMsgId) != epoch || MsgIdUtil.msgIdToEpoch(toMsgId) != epoch) return@sequence
        val ps = state(partition)
        val fromOffset = msgIdToOffset(fromMsgId)
        val toOffset = msgIdToOffset(toMsgId)
        if (fromOffset > ps.committedOffset.value || fromOffset >= toOffset) return@sequence

        FileChannel.open(ps.logFilePath).use { ch ->
            ch.position(fromOffset)
            while (ch.position() < ch.size()) {
                val record = ch.readMessage() ?: continue
                if (record.logOffset >= toOffset) break
                yield(record)
            }
        }
    }

    private fun PartitionState.readRange(afterOffset: LogOffset, toOffset: LogOffset): List<Record<M>> =
        rethrowingChannelInterrupts {
            FileChannel.open(logFilePath).use { fileCh ->
                if (afterOffset >= 0) {
                    fileCh.position(afterOffset)
                    fileCh.readMessage()
                }

                buildList {
                    while (fileCh.position() <= toOffset) fileCh.readMessage()?.let { add(it) }
                }
            }
        }

    override suspend fun <R> withTail(
        partition: Int,
        afterMsgId: MessageId,
        action: suspend (Tail<M>) -> R,
    ): R {
        val ps = state(partition)
        var latestCompletedOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

        return action(object : Tail<M> {
            override suspend fun poll(timeout: Duration): List<Record<M>> {
                val targetOffset = ps.committedOffset.value.takeIf { it > latestCompletedOffset }
                    ?: (if (timeout <= Duration.ZERO) null
                    else withTimeoutOrNull(timeout) { ps.committedOffset.first { it > latestCompletedOffset } })
                    ?: return emptyList()

                return runInterruptible(Dispatchers.IO) {
                    ps.readRange(latestCompletedOffset, targetOffset)
                }.also { records ->
                    check(records.isNotEmpty()) {
                        "LocalLog committed offset $targetOffset yielded no records after $latestCompletedOffset"
                    }
                    latestCompletedOffset = records.last().logOffset
                }
            }
        })
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        for (ps in partitionStates) ps.logFileChannel.close()
    }

    /**
     * Used to set configuration options for a local directory based XTDB Log.
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    log = localLog(Path("test-path")) {
     *      instantSource = InstantSource.system()
     *    }
     *    ...
     * }
     * ```
     */
    @SerialName("!Local")
    @Serializable
    data class Factory @JvmOverloads constructor(
        val path: Path,
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0,
        var useInstantSourceForNonTx: Boolean = false,
        @Transient var coroutineContext: CoroutineContext = Dispatchers.IO
    ) : Log.Factory {

        @Suppress("unused")
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }
        fun useInstantSourceForNonTx() = apply { this.useInstantSourceForNonTx = true }
        fun coroutineContext(coroutineContext: CoroutineContext) = apply { this.coroutineContext = coroutineContext }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            LocalLog(path, SourceMessage.Codec, instantSource, epoch, useInstantSourceForNonTx, coroutineContext, partitions = partitions)

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLocalLog(path, SourceMessage.Codec, epoch, partitions = partitions)

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            LocalLog(path, ReplicaMessage.Codec, instantSource, epoch, useInstantSourceForNonTx, coroutineContext, baseFileName = "REPLICA_LOG", partitions = partitions)

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLocalLog(path, ReplicaMessage.Codec, epoch, baseFileName = "REPLICA_LOG", partitions = partitions)

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.localLog = localLog {
                this.path = this@Factory.path.toString()
                this.epoch = this@Factory.epoch
            }
        }
    }
}
