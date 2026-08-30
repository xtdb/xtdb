package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.selects.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log.*
import xtdb.database.proto.DatabaseConfig
import xtdb.util.MsgIdUtil
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.database.proto.inMemoryLog
import xtdb.types.LogOffset
import xtdb.types.MessageId
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import kotlin.time.Duration

class InMemoryLog<M> @JvmOverloads constructor(
    private val instantSource: InstantSource,
    override val epoch: Int,
    val partitions: Int = 1,
) : Log<M> {

    @SerialName("!InMemory")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0,
    ) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            InMemoryLog<SourceMessage>(instantSource, epoch, partitions)

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openSourceLog(remotes, partitions))

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            InMemoryLog<ReplicaMessage>(instantSource, epoch, partitions)

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openReplicaLog(remotes, partitions))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.inMemoryLog = inMemoryLog {
                this.epoch = this@Factory.epoch
            }
        }
    }

    companion object {
        private const val REPLAY_BUFFER_SIZE = 4096
    }

    // A msgId embeds the epoch but not the partition — partition is implicit in *which partition it
    // came from*.
    private inner class PartitionState {
        val committedCh = MutableSharedFlow<Record<M>>(replay = REPLAY_BUFFER_SIZE)
        val mutex = Mutex()

        @Volatile
        var latestSubmittedOffset: LogOffset = -1
    }

    private val partitionStates = List(partitions) { PartitionState() }

    private fun state(partition: Int): PartitionState =
        partitionStates.getOrNull(partition)
            ?: error("no such partition $partition (partitions=$partitions)")

    override fun latestSubmittedOffset(partition: Int): LogOffset = state(partition).latestSubmittedOffset

    // Mutex ensures offset assignment + emission are atomic per partition,
    // so subscribers always see records in offset order.
    override suspend fun appendMessage(message: M, partition: Int): MessageMetadata {
        val ps = state(partition)
        return ps.mutex.withLock {
            val ts =
                if (message is SourceMessage.Tx || message is SourceMessage.LegacyTx) instantSource.instant() else Instant.now()
            val record = Record(epoch, ++ps.latestSubmittedOffset, ts.truncatedTo(MICROS), message)
            ps.committedCh.emit(record)
            MessageMetadata(epoch, record.logOffset, ts.truncatedTo(MICROS))
        }
    }

    override fun readLastMessage(partition: Int): M? = null

    override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId) = sequence {
        if (MsgIdUtil.msgIdToEpoch(fromMsgId) != epoch || MsgIdUtil.msgIdToEpoch(toMsgId) != epoch) return@sequence
        val fromOffset = msgIdToOffset(fromMsgId)
        val toOffset = msgIdToOffset(toMsgId)
        for (rec in state(partition).committedCh.replayCache) {
            if (rec.logOffset >= toOffset) break
            if (rec.logOffset >= fromOffset) yield(rec)
        }
    }

    override suspend fun <R> withTail(
        partition: Int, afterMsgId: MessageId, action: suspend (Tail<M>) -> R
    ): R = coroutineScope {
        var latestCompletedOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

        val committed = state(partition).committedCh
            .dropWhile { it.logOffset <= latestCompletedOffset }
            .produceIn(this)

        try {
            action(object : Tail<M> {
                override suspend fun poll(timeout: Duration): List<Record<M>> {
                    val first = if (timeout <= Duration.ZERO) {
                        committed.tryReceive().getOrNull()
                    } else {
                        withTimeoutOrNull(timeout) { committed.receive() }
                    } ?: return emptyList()

                    return buildList {
                        add(first)
                        while (true) add(committed.tryReceive().getOrNull() ?: break)
                    }.also { records ->
                        check(records.first().logOffset == latestCompletedOffset + 1) {
                            "InMemoryLog replay buffer rolled past offset ${latestCompletedOffset + 1}"
                        }
                        latestCompletedOffset = records.last().logOffset
                    }
                }
            })
        } finally {
            committed.cancel()
        }
    }

    override fun close() {}
}
