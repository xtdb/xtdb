package xtdb.indexer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import xtdb.api.log.Log
import xtdb.api.log.Log.MessageMetadata
import xtdb.api.log.Log.Record
import xtdb.types.LogOffset
import xtdb.types.MessageId
import xtdb.util.MsgIdUtil
import xtdb.util.debug
import xtdb.util.logger
import java.time.Instant.now
import kotlin.time.Duration
import kotlin.random.Random

private val LOG = SimLog::class.logger

internal class SimLog<M>(private val name: String, private val rand: Random) : Log<M> {
    override val epoch: Int get() = 0

    private var latestSubmittedOffset0: LogOffset = -1

    override fun latestSubmittedOffset(partition: Int): LogOffset = latestSubmittedOffset0

    private var latestEnqueuedOffset: LogOffset = -1

    /** Parents the durability coroutines, so [close] ends them and a cancelled caller does not. */
    private val job = SupervisorJob()

    /**
     * Records whose order is fixed but which are not yet durable, oldest first.
     *
     * They are deliberately not on [topic]: a real log acknowledges a record before it exposes it, so
     * a reader that could see one from here would see a record that might still fail.
     */
    private val inFlight = ArrayDeque<Pair<Record<M>, CompletableDeferred<MessageMetadata>>>()

    class Consumer<M>(var nextOffset: Int) {
        val wake = Channel<Unit>(Channel.CONFLATED)

        // Set only by reportTip. A wake that merely found nothing to deliver is not a tip report: the
        // wakes are how the sim drives delivery at all, so treating each barren one as an election
        // would make every await of the log a leadership change.
        var reportTip = false
    }

    val consumers = mutableSetOf<Consumer<M>>()

    val topic = mutableListOf<Record<M>>()

    override suspend fun enqueueMessage(message: M, partition: Int): Deferred<MessageMetadata> {
        val offset = ++latestEnqueuedOffset
        val ts = now()
        LOG.debug("${name}/enqueue: offset=$offset message=${message!!::class.simpleName}")

        val handle = CompletableDeferred<MessageMetadata>()
        inFlight += Record(epoch, offset, ts, message) to handle

        // On the caller's dispatcher - the sim's, so the seed picks when this runs relative to everything
        // else - but under this log's job rather than the caller's, because a record whose caller was
        // cancelled still lands. That is what makes the handle safe to drop.
        CoroutineScope(currentCoroutineContext().minusKey(Job) + job).launch { commit() }

        return handle
    }

    /** Makes the oldest in-flight record durable, in the order [enqueueMessage] fixed. */
    private fun commit() {
        val (record, handle) = inFlight.removeFirstOrNull() ?: return
        LOG.debug("${name}/commit: offset=${record.logOffset}")
        topic += record
        latestSubmittedOffset0 = record.logOffset
        consumers.forEach<Consumer<M>> { it.wake.trySend(Unit) }
        handle.complete(MessageMetadata(epoch, record.logOffset, record.logTimestamp))
    }

    override fun readLastMessage(partition: Int): M? = topic.lastOrNull()?.message

    override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId): Sequence<Record<M>> {
        val fromOffset = MsgIdUtil.msgIdToOffset(fromMsgId).toInt()
        val toOffset = MsgIdUtil.msgIdToOffset(toMsgId).toInt()
        return topic.subList(fromOffset.coerceAtLeast(0), toOffset.coerceAtMost(topic.size)).asSequence()
    }

    /** [Log.Tail.poll] waits for [reportTip] because the seeded scheduler has no clock. */
    override suspend fun <R> withTail(
        partition: Int,
        afterMsgId: MessageId,
        action: suspend (Log.Tail<M>) -> R,
    ): R {
        val startOffset = (MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId) + 1).toInt()
        LOG.debug("$name/withTail: startOffset=$startOffset topicSize=${topic.size}")
        val consumer = Consumer<M>(startOffset)
        consumers += consumer
        consumer.wake.trySend(Unit)

        try {
            return action(object : Log.Tail<M> {
                override suspend fun poll(timeout: Duration): List<Record<M>> {
                    while (true) {
                        consumer.wake.receive()
                        yield()

                        val nextOffset = consumer.nextOffset
                        val lag = topic.size - nextOffset

                        if (lag == 0) {
                            if (!consumer.reportTip) continue
                            consumer.reportTip = false
                            return emptyList()
                        }

                        val messageCount = rand.nextInt(1, lag + 1)
                        LOG.debug("$name/consumer: delivering $messageCount record(s) [$nextOffset..${nextOffset + messageCount - 1}] (lag=$lag)")
                        consumer.nextOffset += messageCount

                        if (consumer.nextOffset < topic.size) consumer.wake.trySend(Unit)

                        return topic.subList(nextOffset, nextOffset + messageCount).toList()
                    }
                }
            })
        } finally {
            LOG.debug("$name/withTail: closing plain subscription")
            consumers -= consumer
        }
    }

    /**
     * Asks every open tail to report the log's tip once it has nothing left to deliver.
     *
     * The sim's stand-in for a poll that timed out, and with it for an election: a real timeout would need
     * a clock the seeded scheduler does not have, but the *observation* it produces is the same one.
     */
    fun reportTip() = consumers.forEach { it.reportTip = true; it.wake.trySend(Unit) }

    /**
     * Waits until all active plain consumers have processed all messages currently on the topic.
     */
    suspend fun awaitAllDelivered() {
        while (inFlight.isNotEmpty() || consumers.any { it.nextOffset < topic.size }) {
            consumers.forEach { it.wake.trySend(Unit) }
            yield()
        }
    }

    override fun close() = job.cancel()
}
