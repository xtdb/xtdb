package xtdb.indexer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import xtdb.api.log.Log
import xtdb.types.LogOffset
import xtdb.types.MessageId
import xtdb.util.MsgIdUtil
import kotlin.coroutines.coroutineContext
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import java.time.Instant
import kotlin.random.Random

private val LOG = SimLog::class.logger

internal class SimLog<M>(private val name: String, private val rand: Random) : Log<M> {
    override val epoch: Int get() = 0

    private var latestSubmittedOffset0: LogOffset = -1

    override fun latestSubmittedOffset(partition: Int): LogOffset = latestSubmittedOffset0

    class PlainConsumer<M>(val proc: Log.RecordProcessor<M>, var nextOffset: Int, val job: Job)

    val plainConsumers = mutableSetOf<PlainConsumer<M>>()

    val topic = mutableListOf<Log.Record<M>>()

    val wakePlainConsumers = Channel<Unit>(Channel.CONFLATED)

    // A quiet tick: the sim's stand-in for a poll interval elapsing. Consumers with nothing left to read
    // are handed an empty batch — the only evidence a log is quiet, and the only thing an election
    // timeout can be measured across. The sim decides when one happens (advancing its clock in step),
    // because the dispatcher has no virtual time to elapse.
    private val tickTrigger = Channel<Unit>(Channel.CONFLATED)

    fun tick() {
        tickTrigger.trySend(Unit)
    }

    /** Delivers records to lagging consumers, and an empty batch to caught-up ones on a quiet tick. */
    suspend fun plainConsumerLoop() {
        while (true) {
            val quietTick = select {
                wakePlainConsumers.onReceive { false }
                tickTrigger.onReceive { true }
            }
            yield()

            for (consumer in plainConsumers.toList()) {
                if (!consumer.job.isActive) continue
                val nextOffset = consumer.nextOffset
                val lag = topic.size - nextOffset
                val messageCount = if (lag > 0) rand.nextInt(1, lag + 1) else 0

                if (messageCount == 0 && !quietTick) continue
                if (messageCount > 0)
                    LOG.debug("$name/plainConsumer: delivering $messageCount record(s) [$nextOffset..${nextOffset + messageCount - 1}] (lag=$lag)")

                try {
                    consumer.proc.processRecords(topic.subList(nextOffset, nextOffset + messageCount).toList())
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    LOG.error(e, "$name/plainConsumer: processRecords failed")
                    throw e
                }
                consumer.nextOffset += messageCount
            }

            if (plainConsumers.any { it.job.isActive && it.nextOffset < topic.size })
                wakePlainConsumers.send(Unit)
        }
    }

    companion object {
        fun CoroutineScope.launchSimLog(log: SimLog<*>) {
            LOG.debug("${log.name}: starting loops")

            launch(CoroutineName("SimLog/plainConsumers")) { log.plainConsumerLoop() }
        }
    }

    private fun appendSync(message: M): Log.MessageMetadata {
        val offset = ++latestSubmittedOffset0
        val ts = Instant.now()
        LOG.debug("$name/append: offset=$offset message=${message!!::class.simpleName}")
        topic += Log.Record(epoch, offset, ts, message)
        wakePlainConsumers.trySend(Unit)
        return Log.MessageMetadata(epoch, offset, ts)
    }

    override suspend fun appendMessage(message: M, partition: Int): Log.MessageMetadata =
        appendSync(message)

    override fun readLastMessage(partition: Int): M? = topic.lastOrNull()?.message

    override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId): Sequence<Log.Record<M>> {
        val fromOffset = MsgIdUtil.msgIdToOffset(fromMsgId).toInt()
        val toOffset = MsgIdUtil.msgIdToOffset(toMsgId).toInt()
        return topic.subList(fromOffset.coerceAtLeast(0), toOffset.coerceAtMost(topic.size)).asSequence()
    }

    override suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: Log.RecordProcessor<M>) {
        val startOffset = (MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId) + 1).toInt()
        LOG.debug("$name/tailAll: startOffset=$startOffset topicSize=${topic.size}")
        val consumer = PlainConsumer(processor, startOffset, coroutineContext.job)
        plainConsumers += consumer

        if (consumer.nextOffset < topic.size)
            wakePlainConsumers.trySend(Unit)

        try {
            awaitCancellation()
        } finally {
            LOG.debug("$name/tailAll: closing plain subscription")
            plainConsumers -= consumer
        }
    }

    /** Every active plain consumer has been delivered everything currently on the topic. */
    val allDelivered get() = plainConsumers.all { !it.job.isActive || it.nextOffset >= topic.size }

    override fun close() = Unit
}
