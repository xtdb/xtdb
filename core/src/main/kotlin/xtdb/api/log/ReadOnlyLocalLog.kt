package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import xtdb.api.log.Log.*
import xtdb.api.error.Incorrect
import xtdb.types.LogOffset
import xtdb.types.MessageId
import xtdb.util.MsgIdUtil
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.time.InstantUtil.fromMicros
import java.io.DataInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY
import java.nio.file.WatchKey
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.exists
import kotlin.io.path.name
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.Int.Companion.SIZE_BYTES as INT_BYTES
import kotlin.Long.Companion.SIZE_BYTES as LONG_BYTES

/**
 * A read-only version of LocalLog that watches the log file for new messages
 * written by another process (the primary cluster).
 *
 * @suppress
 */
class ReadOnlyLocalLog<M> @JvmOverloads constructor(
    private val rootPath: Path,
    private val codec: MessageCodec<M>,
    override val epoch: Int,
    coroutineContext: CoroutineContext = Dispatchers.Default,
    private val baseFileName: String = "LOG",
    val partitions: Int = 1,
) : Log<M> {

    private val scope = CoroutineScope(coroutineContext)

    companion object {
        private fun messageSizeBytes(size: Int) = 1 + INT_BYTES + LONG_BYTES + size + LONG_BYTES
        private const val RECORD_SEPARATOR = 0x1E.toByte()

        private fun readLatestSubmittedOffset(logFilePath: Path): LogOffset {
            if (!logFilePath.exists()) return -1

            return FileChannel.open(logFilePath, READ).use { ch ->
                val chSize = ch.size()
                if (chSize == 0L) return -1

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

    // See LocalLog.fileNameFor — same nested-for-N>1, plain-file-for-N=1 layout.
    private fun fileNameFor(partition: Int): String =
        if (partitions == 1) baseFileName else "$baseFileName/$partition"

    private fun logFilePath(partition: Int): Path = rootPath.resolve(fileNameFor(partition))

    private fun requirePartition(partition: Int) {
        require(partition in 0 until partitions) { "no such partition $partition (partitions=$partitions)" }
    }

    override fun latestSubmittedOffset(partition: Int): LogOffset {
        requirePartition(partition)
        return readLatestSubmittedOffset(logFilePath(partition))
    }

    override suspend fun appendMessage(message: M, partition: Int): MessageMetadata =
        throw Incorrect("Cannot append to read-only database log")

    override fun openAtomicProducer(transactionalId: String, partition: Int) =
        throw Incorrect("Cannot open atomic producer on read-only database log")

    override fun readLastMessage(partition: Int): M? {
        val latest = latestSubmittedOffset(partition)
        if (latest < 0) return null

        return FileChannel.open(logFilePath(partition), READ).use { ch ->
            ch.position(latest)
            ch.readMessage()?.message
        }
    }

    override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId) = sequence {
        if (MsgIdUtil.msgIdToEpoch(fromMsgId) != epoch || MsgIdUtil.msgIdToEpoch(toMsgId) != epoch) return@sequence
        requirePartition(partition)
        val filePath = logFilePath(partition)
        val latest = readLatestSubmittedOffset(filePath)
        val fromOffset = msgIdToOffset(fromMsgId)
        val toOffset = msgIdToOffset(toMsgId)
        if (fromOffset > latest || fromOffset >= toOffset) return@sequence

        FileChannel.open(filePath, READ).use { ch ->
            ch.position(fromOffset)
            while (ch.position() < ch.size()) {
                val record = ch.readMessage() ?: continue
                if (record.logOffset >= toOffset) break
                yield(record)
            }
        }
    }

    override suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: Log.RecordProcessor<M>) = coroutineScope {
        requirePartition(partition)
        val filePath = logFilePath(partition)
        // Watch the partition file's parent — rootPath at N=1, rootPath/LOG at N>1 — because WatchService
        // events are direct-children only, not recursive. Match against the file's last segment (its name
        // in that parent), which is baseFileName at N=1 and the partition index at N>1.
        val watchDir = filePath.parent
        val watchedName = filePath.fileName.toString()
        val ch = Channel<Log.Record<M>>(100)

        launch {
            var currentOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

            FileSystems.getDefault().newWatchService().use { watchService ->
                // Idempotent — the writer's process may not have created the parent yet at N>1.
                java.nio.file.Files.createDirectories(watchDir)
                watchDir.register(watchService, ENTRY_MODIFY)

                try {
                    readNewMessages(filePath, currentOffset, ch)?.let { currentOffset = it }

                    while (true) {
                        val key: WatchKey = runInterruptible(Dispatchers.IO) { watchService.take() }

                        var logModified = false
                        for (event in key.pollEvents()) {
                            val changedPath = event.context() as? Path
                            if (changedPath?.name == watchedName) {
                                logModified = true
                            }
                        }

                        if (logModified) {
                            readNewMessages(filePath, currentOffset, ch)?.let { currentOffset = it }
                        }

                        if (!key.reset()) {
                            break
                        }
                    }
                } catch (_: ClosedByInterruptException) {
                    cancel()
                } catch (_: InterruptedException) {
                    cancel()
                }
            }
        }

        while (isActive) {
            val msg = withTimeoutOrNull(1.minutes) {
                ch.receiveCatching().let { if (it.isClosed) null else it.getOrThrow() }
            }
            if (msg != null) processor.processRecords(listOf(msg))
        }
    }

    override suspend fun openGroupSubscription(listener: Log.SubscriptionListener<M>) = coroutineScope {
        for (p in 0 until partitions) {
            launch {
                try {
                    listener.launchTransition(p).await()
                    val spec = listener.commitLeader(p)
                    tailAll(p, spec.afterMsgId, spec.processor)
                } finally {
                    withContext(NonCancellable) { listener.demoteLeader(p) }
                }
            }
        }
    }

    private suspend fun readNewMessages(logFilePath: Path, currentOffset: LogOffset, ch: Channel<Log.Record<M>>): LogOffset? {
        val newLatestOffset = readLatestSubmittedOffset(logFilePath)

        if (newLatestOffset <= currentOffset) return null

        var lastOffset = currentOffset
        FileChannel.open(logFilePath, READ).use { fileCh ->
            if (currentOffset >= 0) {
                fileCh.position(currentOffset)
                fileCh.readMessage()
            }

            while (fileCh.position() <= newLatestOffset) {
                val record = fileCh.readMessage()
                if (record != null) {
                    ch.send(record)
                    lastOffset = record.logOffset
                }
            }
        }

        return lastOffset
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
    }
}
