package xtdb.indexer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import xtdb.api.log.LeaderTerm
import xtdb.api.log.Log
import xtdb.types.LogOffset
import xtdb.types.MessageId
import xtdb.util.MsgIdUtil
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

    /**
     * A consumer that participates in leader election (Kafka consumer group semantics).
     */
    class GroupConsumer<M>(val listener: Log.SubscriptionListener<M>) {
        var tailSpec: Log.TailSpec<M>? = null
        var nextOffset = 0
    }

    /** A plain consumer, which receives every record independently of the consumer group. */
    class PlainConsumer<M>(var nextOffset: Int) {
        val wake = Channel<Unit>(Channel.CONFLATED)
    }

    /** The consumer being promoted plus its in-flight transition handle — see [pending]. */
    class Pending<M>(val leader: GroupConsumer<M>, val transition: Deferred<Log.TailSpec<M>>)

    val groupConsumers = mutableSetOf<GroupConsumer<M>>()
    val plainConsumers = mutableSetOf<PlainConsumer<M>>()

    val topic = mutableListOf<Log.Record<M>>()

    val wakeLeader = Channel<Unit>(Channel.CONFLATED)
    val rebalanceTrigger = Channel<Unit>(Channel.CONFLATED)
    private val termCounter = java.util.concurrent.atomic.AtomicLong(0)

    var leader: GroupConsumer<M>? = null

    // An in-flight follower→leader transition (the consumer being promoted + its transition handle) —
    // one field so the two can't drift. Launched off the serialization point (mirroring Kafka's
    // off-poll-thread transition), installed by commitPendingLeader when it completes, and
    // cancel-and-joined by the next rebalance — which drives the LogProcessor's own recovery (re-follow).
    var pending: Pending<M>? = null

    /**
     * Unified group loop: delivers records and handles rebalances in a single coroutine.
     * Rebalances and record delivery are serialized via `select` — a rebalance can only
     * fire between `processRecords` calls, matching real Kafka consumer-thread semantics
     * where rebalance callbacks fire inside `poll()`, never during record processing.
     */
    suspend fun groupLoop() {
        while (true) {
            // Rebalance branch first: Kafka processes pending rebalances before fetching records.
            select {
                rebalanceTrigger.onReceive { doRebalance() }
                wakeLeader.onReceive { deliverGroupRecords() }
                // Install the pending leader once its off-thread transition completes (the sim's
                // equivalent of Kafka's TransitionComplete arriving back on the poll thread).
                pending?.let { it.transition.onAwait { spec -> commitPendingLeader(spec) } }
            }
            yield()
        }
    }

    private suspend fun deliverGroupRecords() {
        this.leader?.let { leader ->
            val tailSpec = leader.tailSpec
                ?: run {
                    LOG.debug("$name/processMessages: leader has no tail spec, skipping")
                    return@let
                }

            val nextOffset = leader.nextOffset
            val lag = topic.size - nextOffset

            if (lag > 0) {
                val messageCount = rand.nextInt(1, lag + 1)
                LOG.debug("$name/processMessages: delivering $messageCount group record(s) [$nextOffset..${nextOffset + messageCount - 1}] (lag=$lag)")
                try {
                    tailSpec.processor.processRecords(topic.subList(nextOffset, nextOffset + messageCount).toList())
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    LOG.error(e, "$name/groupConsumer: processRecords failed")
                    throw e
                }
                leader.nextOffset += messageCount
            }

            if (leader.nextOffset < topic.size)
                wakeLeader.send(Unit)
        }
    }

    private suspend fun doRebalance() {
        LOG.debug("$name/chooseLeader: rebalance triggered (${groupConsumers.size} consumers)")

        // Tear down whatever we hold — a leader we're feeding, or an in-flight transition. For the pending
        // one, cancel-and-join (bounded; drives the LogProcessor's recovery) then demote, which tears down
        // a leader the transition had already built or is a no-op if recovery re-followed. Mirrors Kafka
        // revoke.
        pending?.let { p ->
            p.transition.cancelAndJoin()
            p.leader.listener.demoteLeader(0)
        }
        leader?.let { old ->
            LOG.debug("$name/chooseLeader: revoking old leader")
            old.listener.demoteLeader(0)
            old.tailSpec = null
        }
        pending = null
        leader = null

        if (groupConsumers.isNotEmpty()) {
            val newLeader = groupConsumers.random(rand)
            LOG.debug("$name/chooseLeader: launching transition for new leader")
            pending = Pending(newLeader, newLeader.listener.transitionToLeader(0, LeaderTerm.of(0, termCounter.incrementAndGet())))
            // installed by commitPendingLeader (group-loop select clause) once the transition completes
        } else {
            LOG.debug("$name/chooseLeader: no consumers, no leader elected")
        }
    }

    // Group loop (serialization point): start feeding the leader its transition built.
    private fun commitPendingLeader(tailSpec: Log.TailSpec<M>) {
        val newLeader = pending?.leader ?: return
        pending = null
        LOG.debug("$name/chooseLeader: committing new leader")
        newLeader.tailSpec = tailSpec
        newLeader.nextOffset = (MsgIdUtil.afterMsgIdToOffset(epoch, tailSpec.afterMsgId) + 1).toInt()
        leader = newLeader
        wakeLeader.trySend(Unit)
    }

    companion object {
        fun CoroutineScope.launchSimLog(log: SimLog<*>) {
            LOG.debug("${log.name}: starting loops")

            launch(CoroutineName("SimLog/group")) { log.groupLoop() }
        }
    }

    private fun appendSync(message: M): Log.MessageMetadata {
        val offset = ++latestSubmittedOffset0
        val ts = Instant.now()
        LOG.debug("$name/append: offset=$offset message=${message!!::class.simpleName}")
        topic += Log.Record(epoch, offset, ts, message)
        wakeLeader.trySend(Unit)
        plainConsumers.forEach { it.wake.trySend(Unit) }
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

    /**
     * Reads on the caller's own coroutine, as every real [Log] does.
     *
     * A shared loop delivering into each consumer from outside its job cannot be cancelled per
     * consumer: [Log.RecordProcessor.processRecords] suspends until the consumer has applied the
     * batch, so cancelling one mid-delivery would leave that loop suspended forever and starve
     * every other subscription behind it.
     */
    override suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: Log.RecordProcessor<M>) {
        val startOffset = (MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId) + 1).toInt()
        LOG.debug("$name/tailAll: startOffset=$startOffset topicSize=${topic.size}")
        val consumer = PlainConsumer<M>(startOffset)
        plainConsumers += consumer
        consumer.wake.trySend(Unit)

        try {
            while (true) {
                consumer.wake.receive()
                yield()

                val nextOffset = consumer.nextOffset
                val lag = topic.size - nextOffset
                if (lag == 0) continue

                val messageCount = rand.nextInt(1, lag + 1)
                LOG.debug("$name/plainConsumer: delivering $messageCount record(s) [$nextOffset..${nextOffset + messageCount - 1}] (lag=$lag)")
                try {
                    processor.processRecords(topic.subList(nextOffset, nextOffset + messageCount).toList())
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    LOG.error(e, "$name/plainConsumer: processRecords failed")
                    throw e
                }
                consumer.nextOffset += messageCount

                if (consumer.nextOffset < topic.size) consumer.wake.trySend(Unit)
            }
        } finally {
            LOG.debug("$name/tailAll: closing plain subscription")
            plainConsumers -= consumer
        }
    }

    private fun newGroupConsumer(listener: Log.SubscriptionListener<M>): GroupConsumer<M> {
        LOG.debug("$name: new group consumer joining (total will be ${groupConsumers.size + 1})")
        return GroupConsumer(listener).also {
            groupConsumers += it
            rebalanceTrigger.trySend(Unit)
        }
    }

    private fun groupConsumerClosed(c: GroupConsumer<M>) {
        LOG.debug("$name: group consumer leaving (total will be ${groupConsumers.size - 1})")
        groupConsumers -= c
        rebalanceTrigger.trySend(Unit)
    }

    override suspend fun openGroupSubscription(listener: Log.SubscriptionListener<M>) {
        val consumer = newGroupConsumer(listener)
        try {
            awaitCancellation()
        } finally {
            LOG.debug("$name/groupSubscription: closing")
            groupConsumerClosed(consumer)
        }
    }

    /**
     * Waits until all active plain consumers have processed all messages currently on the topic.
     */
    suspend fun awaitAllDelivered() {
        while (plainConsumers.any { it.nextOffset < topic.size }) {
            plainConsumers.forEach { it.wake.trySend(Unit) }
            yield()
        }
    }

    override fun close() = Unit
}
