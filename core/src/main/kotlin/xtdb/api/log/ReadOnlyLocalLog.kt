package xtdb.api.log

import kotlinx.coroutines.*
import xtdb.api.log.Log.*
import xtdb.error.Incorrect
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
import kotlin.time.Duration.Companion.seconds
import kotlin.Int.Companion.SIZE_BYTES as INT_BYTES
import kotlin.Long.Companion.SIZE_BYTES as LONG_BYTES

/**
 * A read-only version of LocalLog that watches the log file for new messages
 * written by another process (the primary cluster).
 */
class ReadOnlyLocalLog(
    rootPath: Path,
    override val epoch: Int,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Log {

    private val scope = CoroutineScope(coroutineContext)
    private val logFilePath = rootPath.resolve("LOG")
    private val watchService = FileSystems.getDefault().newWatchService()

    init {
        // Watch the parent directory for modifications to the LOG file
        rootPath.register(watchService, ENTRY_MODIFY)
    }

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

        private fun FileChannel.readMessage(): Record? {
            val pos = position()
            val headerBuf = ByteBuffer.allocateDirect(1 + INT_BYTES + LONG_BYTES)
                .also { read(it); it.flip() }

            check(headerBuf.get() == RECORD_SEPARATOR) { "log file corrupted at $pos - expected record separator" }
            val size = headerBuf.getInt()

            val message =
                Message.parse(ByteBuffer.allocate(size).also { read(it); it.flip() }.array())
                    ?: return null

            return Record(pos, fromMicros(headerBuf.getLong()), message)
                .also { position(pos + messageSizeBytes(size)) }
        }
    }

    override val latestSubmittedOffset: LogOffset
        get() = readLatestSubmittedOffset(logFilePath)

    override fun appendMessage(message: Message) =
        throw Incorrect("Cannot append to read-only database log")

    override fun openAtomicProducer(transactionalId: String) =
        throw Incorrect("Cannot open atomic producer on read-only database log")

    override fun readLastMessage(): Message? {
        if (latestSubmittedOffset < 0) return null

        return FileChannel.open(logFilePath, READ).use { ch ->
            ch.position(latestSubmittedOffset)
            ch.readMessage()?.message
        }
    }

    override fun tailAll(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription {
        var currentOffset = latestProcessedOffset

        val subscription = scope.launch(SupervisorJob()) {
            try {
                // First, catch up on any existing messages
                readNewMessages(currentOffset, subscriber)?.let { currentOffset = it }

                // Then watch for new changes
                while (true) {
                    val key: WatchKey = runInterruptible(Dispatchers.IO) { watchService.take() }

                    var logModified = false
                    for (event in key.pollEvents()) {
                        val changedPath = event.context() as? Path
                        if (changedPath?.name == "LOG") {
                            logModified = true
                        }
                    }

                    if (logModified) {
                        readNewMessages(currentOffset, subscriber)?.let { currentOffset = it }
                    }

                    if (!key.reset()) {
                        break // Directory no longer accessible
                    }
                }
            } catch (_: ClosedByInterruptException) {
                cancel()
            } catch (_: InterruptedException) {
                cancel()
            }
        }

        return Subscription { runBlocking { withTimeout(5.seconds) { subscription.cancelAndJoin() } } }
    }

    override fun subscribe(subscriber: GroupSubscriber): Subscription {
        val offsets = subscriber.onPartitionsAssigned(listOf(0))
        val nextOffset = offsets[0] ?: 0L
        val subscription = tailAll(subscriber, nextOffset - 1)
        return Subscription {
            subscription.close()
            subscriber.onPartitionsRevoked(listOf(0))
        }
    }

    private fun readNewMessages(currentOffset: LogOffset, subscriber: Subscriber): LogOffset? {
        val newLatestOffset = readLatestSubmittedOffset(logFilePath)

        if (newLatestOffset <= currentOffset) return null

        var lastOffset = currentOffset
        FileChannel.open(logFilePath, READ).use { ch ->
            // Position after the current offset
            if (currentOffset >= 0) {
                ch.position(currentOffset)
                ch.readMessage() // skip past current
            }

            // Read all new messages
            while (ch.position() <= newLatestOffset) {
                val record = ch.readMessage()
                if (record != null) {
                    subscriber.processRecords(listOf(record))
                    lastOffset = record.logOffset
                }
            }
        }

        return lastOffset
    }

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        watchService.close()
    }
}
