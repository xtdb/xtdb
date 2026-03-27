package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import xtdb.api.log.Log.*
import xtdb.error.Incorrect
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
 */
class ReadOnlyLocalLog<M>(
    private val rootPath: Path,
    private val codec: MessageCodec<M>,
    override val epoch: Int,
    coroutineContext: CoroutineContext = Dispatchers.Default,
    private val logFileName: String = "LOG"
) : Log<M> {

    private val scope = CoroutineScope(coroutineContext)
    private val logFilePath = rootPath.resolve(logFileName)

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

    override val latestSubmittedOffset: LogOffset
        get() = readLatestSubmittedOffset(logFilePath)

    override suspend fun appendMessage(message: M): MessageMetadata =
        throw Incorrect("Cannot append to read-only database log")

    override fun openAtomicProducer(transactionalId: String) =
        throw Incorrect("Cannot open atomic producer on read-only database log")

    override fun readLastMessage(): M? {
        if (latestSubmittedOffset < 0) return null

        return FileChannel.open(logFilePath, READ).use { ch ->
            ch.position(latestSubmittedOffset)
            ch.readMessage()?.message
        }
    }

    override fun readRecords(fromMsgId: MessageId, toMsgId: MessageId) = sequence {
        if (MsgIdUtil.msgIdToEpoch(fromMsgId) != epoch || MsgIdUtil.msgIdToEpoch(toMsgId) != epoch) return@sequence
        val fromOffset = msgIdToOffset(fromMsgId)
        val toOffset = msgIdToOffset(toMsgId)
        if (fromOffset > latestSubmittedOffset || fromOffset >= toOffset) return@sequence

        FileChannel.open(logFilePath, READ).use { ch ->
            ch.position(fromOffset)
            while (ch.position() < ch.size()) {
                val record = ch.readMessage() ?: continue
                if (record.logOffset >= toOffset) break
                yield(record)
            }
        }
    }

    override fun tailAll(afterMsgId: MessageId, processor: Log.RecordProcessor<M>): Log.Subscription {
        val ch = Channel<Log.Record<M>>(100)

        val producerJob = scope.launch(SupervisorJob()) {
            var currentOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

            FileSystems.getDefault().newWatchService().use { watchService ->
                rootPath.register(watchService, ENTRY_MODIFY)

                try {
                    readNewMessages(currentOffset, ch)?.let { currentOffset = it }

                    while (true) {
                        val key: WatchKey = runInterruptible(Dispatchers.IO) { watchService.take() }

                        var logModified = false
                        for (event in key.pollEvents()) {
                            val changedPath = event.context() as? Path
                            if (changedPath?.name == logFileName) {
                                logModified = true
                            }
                        }

                        if (logModified) {
                            readNewMessages(currentOffset, ch)?.let { currentOffset = it }
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

        val consumerJob = scope.launch(SupervisorJob()) {
            try {
                while (isActive) {
                    val msg = withTimeoutOrNull(1.minutes) {
                        ch.receiveCatching().let { if (it.isClosed) null else it.getOrThrow() }
                    }
                    if (msg != null) processor.processRecords(listOf(msg))
                }
            } finally {
                producerJob.cancelAndJoin()
            }
        }

        return Log.Subscription { runBlocking { withTimeout(5.seconds) { consumerJob.cancelAndJoin() } } }
    }

    override fun openGroupSubscription(listener: Log.SubscriptionListener<M>): Log.Subscription {
        val spec = listener.onPartitionsAssignedSync(listOf(0))
        val sub = spec?.let { tailAll(it.afterMsgId, it.processor) }
        return Subscription {
            sub?.close()
            listener.onPartitionsRevokedSync(listOf(0))
        }
    }

    private suspend fun readNewMessages(currentOffset: LogOffset, ch: Channel<Log.Record<M>>): LogOffset? {
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
