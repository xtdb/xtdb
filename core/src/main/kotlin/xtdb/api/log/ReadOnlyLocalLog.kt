package xtdb.api.log

import kotlinx.coroutines.*
import xtdb.api.log.Log.*
import xtdb.api.error.Incorrect
import xtdb.types.LogOffset
import xtdb.types.MessageId
import xtdb.util.MsgIdUtil
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.rethrowingChannelInterrupts
import xtdb.time.InstantUtil.fromMicros
import java.io.DataInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY
import java.util.concurrent.TimeUnit.NANOSECONDS
import kotlin.io.path.exists
import kotlin.time.Duration
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
    private val baseFileName: String = "LOG",
    val partitions: Int = 1,
) : Log<M> {

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

    override suspend fun <R> withTail(
        partition: Int,
        afterMsgId: MessageId,
        action: suspend (Log.Tail<M>) -> R,
    ): R {
        requirePartition(partition)
        val filePath = logFilePath(partition)
        val watchDir = filePath.parent

        java.nio.file.Files.createDirectories(watchDir)

        return FileSystems.getDefault().newWatchService().use { watchService ->
            watchDir.register(watchService, ENTRY_MODIFY)
            var currentOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

            fun readNewMessages(): List<Log.Record<M>> = rethrowingChannelInterrupts {
                val newLatestOffset = readLatestSubmittedOffset(filePath)
                if (newLatestOffset <= currentOffset) return@rethrowingChannelInterrupts emptyList()

                FileChannel.open(filePath, READ).use { fileCh ->
                    if (currentOffset >= 0) {
                        fileCh.position(currentOffset)
                        fileCh.readMessage()
                    }

                    buildList {
                        while (fileCh.position() <= newLatestOffset) fileCh.readMessage()?.let { add(it) }
                    }
                }.also { records -> records.lastOrNull()?.let { currentOffset = it.logOffset } }
            }

            action(object : Log.Tail<M> {
                override suspend fun poll(timeout: Duration): List<Log.Record<M>> {
                    val started = kotlin.time.TimeSource.Monotonic.markNow()
                    var remaining = timeout

                    while (true) {
                        runInterruptible(Dispatchers.IO) { readNewMessages() }
                            .takeIf { it.isNotEmpty() }
                            ?.let { return it }

                        if (remaining <= Duration.ZERO) return emptyList()

                        val key = runInterruptible(Dispatchers.IO) {
                            watchService.poll(remaining.inWholeNanoseconds, NANOSECONDS)
                        } ?: return emptyList()

                        key.pollEvents()
                        check(key.reset()) { "local log watch is no longer valid" }
                        remaining = timeout - started.elapsedNow()
                    }
                }
            })
        }
    }

    override fun close() = Unit
}
