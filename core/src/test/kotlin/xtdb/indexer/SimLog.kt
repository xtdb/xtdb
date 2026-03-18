package xtdb.indexer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import xtdb.api.log.Log
import xtdb.api.log.LogOffset
import xtdb.api.log.MessageId
import xtdb.util.MsgIdUtil
import kotlin.coroutines.coroutineContext
import xtdb.util.debug
import xtdb.util.logger
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random

private val LOG = SimLog::class.logger

internal class SimLog<M>(private val name: String, ctx: CoroutineContext, private val rand: Random) : Log<M> {
    override val epoch: Int get() = 0

    override var latestSubmittedOffset: LogOffset = -1
        private set

    /**
     * A consumer that participates in leader election (Kafka consumer group semantics).
     */
    class GroupConsumer<M>(val listener: Log.SubscriptionListener<M>) {
        var tailSpec: Log.TailSpec<M>? = null
        var nextOffset = 0
    }

    /**
     * A plain consumer that receives all records independently (not part of the consumer group).
     * Used by the replica log's follower subscription via Log.tailAll.
     */
    class PlainConsumer<M>(val proc: Log.RecordProcessor<M>, var nextOffset: Int, val job: Job)

    val groupConsumers = mutableSetOf<GroupConsumer<M>>()
    val plainConsumers = mutableSetOf<PlainConsumer<M>>()

    val topic = mutableListOf<Log.Record<M>>()

    val wakeLeader = Channel<Unit>(Channel.CONFLATED)
    val wakePlainConsumers = Channel<Unit>(Channel.CONFLATED)
    val rebalanceTrigger = Channel<Unit>(Channel.CONFLATED)

    var leader: GroupConsumer<M>? = null

    val job = Job()
    private val scope = CoroutineScope(ctx + job)

    /**
     * Delivers records to the current group leader.
     */
    suspend fun processMessagesLoop() {
        while (true) {
            wakeLeader.receive()
            yield()

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
                    tailSpec.processor.processRecords(topic.subList(nextOffset, nextOffset + messageCount).toList())
                    leader.nextOffset += messageCount
                }

                if (leader.nextOffset < topic.size)
                    wakeLeader.send(Unit)
            }
        }
    }

    /**
     * Delivers records to all plain (non-group) consumers.
     */
    suspend fun plainConsumerLoop() {
        while (true) {
            wakePlainConsumers.receive()
            yield()

            for (consumer in plainConsumers.toList()) {
                if (!consumer.job.isActive) continue
                val nextOffset = consumer.nextOffset
                val lag = topic.size - nextOffset
                if (lag > 0) {
                    val messageCount = rand.nextInt(1, lag + 1)
                    LOG.debug("$name/plainConsumer: delivering $messageCount record(s) [$nextOffset..${nextOffset + messageCount - 1}] (lag=$lag)")
                    consumer.proc.processRecords(topic.subList(nextOffset, nextOffset + messageCount).toList())
                    consumer.nextOffset += messageCount
                }
            }

            if (plainConsumers.any { it.job.isActive && it.nextOffset < topic.size })
                wakePlainConsumers.send(Unit)
        }
    }

    /**
     * Handles leader election — reacts to consumer join/leave events.
     */
    suspend fun chooseLeaderLoop() {
        while (true) {
            rebalanceTrigger.receive()
            yield()

            LOG.debug("$name/chooseLeader: rebalance triggered (${groupConsumers.size} consumers)")

            leader?.let { old ->
                LOG.debug("$name/chooseLeader: revoking old leader")
                old.listener.onPartitionsRevoked(listOf(0))
                old.tailSpec = null
                leader = null
            }

            if (groupConsumers.isNotEmpty()) {
                val newLeader = groupConsumers.random(rand)
                LOG.debug("$name/chooseLeader: assigning new leader")
                val tailSpec = newLeader.listener.onPartitionsAssigned(listOf(0))
                newLeader.tailSpec = tailSpec
                if (tailSpec != null) {
                    val startOffset = (MsgIdUtil.afterMsgIdToOffset(epoch, tailSpec.afterMsgId) + 1).toInt()
                    newLeader.nextOffset = startOffset
                }
                leader = newLeader
                wakeLeader.send(Unit)
            } else {
                LOG.debug("$name/chooseLeader: no consumers, no leader elected")
            }
        }
    }

    init {
        LOG.debug("$name: starting loops")
        scope.launch(CoroutineName("SimLog/processMessages")) { processMessagesLoop() }
        scope.launch(CoroutineName("SimLog/plainConsumers")) { plainConsumerLoop() }
        scope.launch(CoroutineName("SimLog/chooseLeader")) { chooseLeaderLoop() }
    }

    private fun appendSync(message: M): Log.MessageMetadata {
        val offset = ++latestSubmittedOffset
        val ts = Instant.now()
        LOG.debug("$name/append: offset=$offset message=${message!!::class.simpleName}")
        topic += Log.Record(epoch, offset, ts, message)
        wakeLeader.trySend(Unit)
        wakePlainConsumers.trySend(Unit)
        return Log.MessageMetadata(epoch, offset, ts)
    }

    override suspend fun appendMessage(message: M): Log.MessageMetadata =
        appendSync(message)

    override fun openAtomicProducer(transactionalId: String) = object : Log.AtomicProducer<M> {
        override fun openTx() = object : Log.AtomicProducer.Tx<M> {
            private val buffer = mutableListOf<Pair<M, CompletableDeferred<Log.MessageMetadata>>>()
            private var isOpen = true

            override fun appendMessage(message: M): CompletableDeferred<Log.MessageMetadata> {
                check(isOpen) { "Transaction already closed" }
                return CompletableDeferred<Log.MessageMetadata>()
                    .also { buffer.add(message to it) }
            }

            override fun commit() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                LOG.debug("$name/atomicProducer($transactionalId): committing ${buffer.size} message(s)")
                for ((message, res) in buffer) {
                    res.complete(appendSync(message))
                }
            }

            override fun abort() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                LOG.debug("$name/atomicProducer($transactionalId): aborting ${buffer.size} message(s)")
                buffer.clear()
            }

            override fun close() {
                if (isOpen) abort()
            }
        }

        override fun close() {}
    }

    override fun readLastMessage(): M? = topic.lastOrNull()?.message

    override fun readRecords(fromMsgId: MessageId, toMsgId: MessageId): Sequence<Log.Record<M>> {
        val fromOffset = MsgIdUtil.msgIdToOffset(fromMsgId).toInt()
        val toOffset = MsgIdUtil.msgIdToOffset(toMsgId).toInt()
        return topic.subList(fromOffset.coerceAtLeast(0), toOffset.coerceAtMost(topic.size)).asSequence()
    }

    override suspend fun tailAll(afterMsgId: MessageId, processor: Log.RecordProcessor<M>) {
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

    override fun close() {
        LOG.debug("$name: closing")
        job.cancel()
        runBlocking {
            LOG.debug("$name: waiting for loops to finish")
            job.join()
            LOG.debug("$name: loops finished")
        }
        LOG.debug("$name: closed")
    }
}
