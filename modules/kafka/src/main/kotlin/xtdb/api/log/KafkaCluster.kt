@file:UseSerializers(
    DurationSerde::class,
    PathSerde::class
)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.onClosed
import kotlinx.coroutines.channels.onSuccess
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import xtdb.DurationSerde
import xtdb.api.PathSerde
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.database.proto.DatabaseConfig
import xtdb.kafka.proto.KafkaLogConfig
import xtdb.kafka.proto.kafkaLogConfig
import xtdb.types.LogOffset
import xtdb.types.MessageId
import xtdb.util.MsgIdUtil.afterMsgIdToOffset
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.close
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn
import java.nio.file.Path
import java.time.Duration
import java.time.Instant.ofEpochMilli
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.io.path.inputStream
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import com.google.protobuf.Any as ProtoAny

private val LOG = KafkaCluster::class.logger

private typealias KafkaConfigMap = Map<String, String>

private object UnitSerializer : Serializer<Unit> {
    override fun serialize(topic: String?, data: Unit) = null
}

private object UnitDeserializer : Deserializer<Unit> {
    override fun deserialize(topic: String?, data: ByteArray) = Unit
}

private fun KafkaConfigMap.openProducer() =
    KafkaProducer(
        mapOf(
            "enable.idempotence" to "true",
            "acks" to "all",
            "compression.type" to "snappy",
            "linger.ms" to "0",
        ) + this,
        UnitSerializer,
        ByteArraySerializer()
    )

private fun KafkaConsumer<*, *>.seekToAfterMsgId(tp: TopicPartition, epoch: Int, afterMsgId: MessageId) {
    val previousOffset = afterMsgIdToOffset(epoch, afterMsgId)
    if (previousOffset < 0L) seekToBeginning(listOf(tp)) else seek(tp, previousOffset + 1)
}

private fun KafkaConfigMap.openConsumer() =
    KafkaConsumer(
        mapOf(
            "enable.auto.commit" to "false",
            "isolation.level" to "read_committed",
            "auto.offset.reset" to "none",
            "partition.assignment.strategy" to "org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
        ) + this,
        UnitDeserializer,
        ByteArrayDeserializer()
    )

private fun AdminClient.ensureTopicExists(topic: String, autoCreate: Boolean) {
    val desc =
        try {
            describeTopics(listOf(topic)).allTopicNames().get()[topic]
        } catch (e: ExecutionException) {
            try {
                throw e.cause ?: e
            } catch (_: UnknownTopicOrPartitionException) {
                null
            } catch (e: Throwable) {
                throw e
            }
        }

    when {
        desc != null -> {
            check(desc.partitions().size == 1) { "Topic $topic must have exactly one partition" }
        }

        autoCreate -> {
            val newTopic = NewTopic(topic, 1, 1)
                .configs(mapOf("message.timestamp.type" to "LogAppendTime"))

            createTopics(listOf(newTopic))
        }

        else -> error("Topic $topic does not exist, auto-create set to false")
    }
}


class KafkaCluster(
    val kafkaConfigMap: KafkaConfigMap,
    private val pollDuration: Duration,
    val schemaRegistryUrl: String? = null,
    val transactionalIdPrefix: String? = null,
    private val groupId: String = "xtdb",
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Remote {
    val producer = kafkaConfigMap.openProducer()
    val scope = CoroutineScope(SupervisorJob() + coroutineContext)

    private sealed interface GroupCommand {
        class Register(val topic: String, val subscription: SharedGroupConsumer.TopicSubscription<*>) : GroupCommand
        class Unregister(val topic: String, val cont: CancellableContinuation<Unit>) : GroupCommand

        // Posted by a transition's off-thread joiner back onto the poll thread, so the committed role
        // change (commitLeader/seek/resume, or eviction on failure) happens at the poll thread's
        // serialization point. Carries the Transitioning state it ran for: if the subscription has since
        // moved on (revoke, reassign) its state is no longer that instance, and the post is dropped (#5773).
        class TransitionComplete(val tp: TopicPartition, val transitioning: Transitioning<*>) : GroupCommand
        class TransitionFailed(val tp: TopicPartition, val transitioning: Transitioning<*>, val cause: Throwable) : GroupCommand
    }

    // The per-subscription leader-election state, held on TopicSubscription (not a side map) so the states
    // and their data are checked by the type system rather than tracked by hand. At this level (not in the
    // inner SharedGroupConsumer) because Kotlin forbids a nested class inside an inner class.
    //   Following       — not leading, no transition in flight (initial, and after a demote/recovery).
    //   Transitioning   — an off-thread follower→leader transition is running; the instance is its own
    //                     identity token, so a stale completion for a superseded transition is dropped.
    //   LeaderCommitted — committed leader; holds the record processor the poll loop feeds.
    private sealed interface ListenerState<M>
    private class Following<M> : ListenerState<M>
    private class Transitioning<M>(val transition: Deferred<Unit>) : ListenerState<M>
    private class LeaderCommitted<M>(val processor: Log.RecordProcessor<M>) : ListenerState<M>

    private inner class SharedGroupConsumer : AutoCloseable {
        private val subscriptions = mutableMapOf<String, TopicSubscription<*>>()
        private val commandCh = Channel<GroupCommand>(Channel.UNLIMITED)

        private val consumer: KafkaConsumer<Unit, ByteArray> =
            kafkaConfigMap.plus(mapOf("group.id" to groupId)).openConsumer()

        inner class TopicSubscription<M>(
            val codec: MessageCodec<M>,
            val epoch: Int,
            val listener: Log.SubscriptionListener<M>,
        ) {
            private val completion = CompletableDeferred<Unit>()
            private var listenerState: ListenerState<M> = Following()

            // expecting a load of change here for multi-part.

            fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                val tp = partitions.single()
                when (val s = listenerState) {
                    // Retained partitions (CooperativeStickyAssignor) fire no callback; a callback while
                    // we already lead would rewind the source tail, so only transition from Following.
                    is LeaderCommitted -> return
                    is Transitioning -> s.transition.cancel()   // supersede an unexpected in-flight transition
                    is Following -> {}
                }

                consumer.pause(listOf(tp))
                // A freshly-assigned partition has no fetch position, and poll() throws
                // NoOffsetForPartitionException for it (auto.offset.reset=none) even while paused — pause
                // suppresses fetching, not position validation. Park it at the end as a placeholder: it's
                // paused (never fetched) and re-seeked to the real offset before resume at commit.
                consumer.seekToEnd(listOf(tp))

                val transition = listener.launchTransition(listOf(tp.partition()))
                val transitioning = Transitioning<M>(transition)
                listenerState = transitioning

                scope.launch {
                    val outcome: GroupCommand? =
                        try {
                            transition.await()
                            GroupCommand.TransitionComplete(tp, transitioning)
                        } catch (_: CancellationException) {
                            null   // revoked / torn down — nothing to commit
                        } catch (e: Throwable) {
                            GroupCommand.TransitionFailed(tp, transitioning, e)
                        }

                    // Only wake the consumer if the command was queued — a closed channel means we're
                    // shutting down, and waking a closing consumer would race its close().
                    if (outcome != null && commandCh.trySend(outcome).isSuccess) consumer.wakeup()
                }
            }

            suspend fun onPartitionRevoked(partitions: Collection<TopicPartition>) {
                // Abandon any in-flight transition first — cancel-and-join is bounded (the catch-up await
                // is cancellable) and lets the transition's own recovery settle `state`. Then demote:
                // Prepared→abandon, Leading→teardown, Following→no-op. Back to Following either way.
                (listenerState as? Transitioning)?.transition?.cancelAndJoin()
                listener.demoteLeader(listOf(partitions.single().partition()))
                listenerState = Following()
            }

            suspend fun processRecords(records: List<ConsumerRecord<*, ByteArray>>) {
                // Only a committed leader has a processor; while Following/Transitioning the partition is
                // paused, so this guard is belt-and-braces.
                val proc = (listenerState as? LeaderCommitted)?.processor ?: return

                proc.processRecords(
                    records.mapNotNull { rec ->
                        codec.decode(rec.value())
                            ?.let { msg -> Log.Record(epoch, rec.offset(), ofEpochMilli(rec.timestamp()), msg) }
                    })
            }

            // Poll thread, at the serialization point: install the prepared leader, seek, resume. Drops a
            // stale completion whose state has moved on (revoke / reassign) — it is no longer this
            // Transitioning instance.
            fun onTransitionComplete(cmd: GroupCommand.TransitionComplete) {
                if (listenerState !== cmd.transitioning) return

                val tailSpec = listener.commitLeader(listOf(cmd.tp.partition()))
                listenerState = LeaderCommitted(tailSpec.processor)
                consumer.seekToAfterMsgId(cmd.tp, epoch, tailSpec.afterMsgId)   // seek before resume — #5633
                consumer.resume(listOf(cmd.tp))
            }

            // Poll thread: a transition failed. Isolate it — evict only this subscription, leaving siblings
            // on the shared consumer running. Its watchers were already poisoned by the transition body.
            fun onTransitionFailed(cmd: GroupCommand.TransitionFailed) {
                if (listenerState !== cmd.transitioning) return
                LOG.error(cmd.cause) { "leader transition failed for ${cmd.tp} — evicting its subscription" }
                evictSubscriptions(listOf(cmd.tp.topic()), cmd.cause)
            }

            // On unregister: complete the subscription so openGroupSubscription returns. Any in-flight
            // transition is a child of the database scope, which is being cancelled — left to it (#5773).
            fun cancel() {
                if (completion.isActive) completion.complete(Unit)
            }

            // Poll thread: abandon this subscription with a failure cause (a failed transition, or poll-loop
            // teardown). Cancel — never join, the poll thread must not block — any in-flight transition, and
            // fail the completion so openGroupSubscription unwinds with the cause. Deliberately no
            // demoteLeader: that would drive a leader→follower transition mid-teardown.
            fun evict(cause: Throwable) {
                (listenerState as? Transitioning)?.transition?.cancel()
                if (completion.isActive) completion.completeExceptionally(cause)
            }

            // Suspend until this subscription ends — normally via cancel (unregister), or exceptionally with
            // the eviction cause, which then propagates out of openGroupSubscription.
            suspend fun await() = completion.await()
        }


        private inline fun launderInterruptedException(block: () -> Unit) =
            try {
                block()
            } catch (e: InterruptedException) {
                throw InterruptException(e)
            }

        private val listener = object : ConsumerRebalanceListener {

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) =
                launderInterruptedException {
                    for ((topic, tps) in partitions.groupBy { it.topic() }) {
                        subscriptions[topic]?.onPartitionsAssigned(tps)
                    }
                }

            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) =
                launderInterruptedException {
                    runBlocking {
                        for ((topic, tps) in partitions.groupBy { it.topic() }) {
                            subscriptions[topic]?.onPartitionRevoked(tps)
                        }
                    }
                }
        }

        private fun applySubscriptions() {
            val topics = subscriptions.keys.toList()

            if (topics.isEmpty()) consumer.unsubscribe() else consumer.subscribe(topics, listener)
        }

        private fun processCommand(cmd: GroupCommand) {
            when (cmd) {
                is GroupCommand.Register -> {
                    check(cmd.topic !in subscriptions) { "Topic ${cmd.topic} already registered" }
                    subscriptions[cmd.topic] = cmd.subscription
                    applySubscriptions()
                }

                is GroupCommand.Unregister -> {
                    // No demote here: the database's scope cancel tears the leader term down (#5773) —
                    // and the in-flight transition, if any, is a child of that scope too, so it's left to it.
                    subscriptions.remove(cmd.topic)?.cancel()
                    applySubscriptions()
                    cmd.cont.resume(Unit)
                }

                is GroupCommand.TransitionComplete ->
                    subscriptions[cmd.tp.topic()]?.onTransitionComplete(cmd)

                is GroupCommand.TransitionFailed ->
                    subscriptions[cmd.tp.topic()]?.onTransitionFailed(cmd)
            }
        }

        private fun evictSubscriptions(topics: Collection<String>, cause: Throwable) {
            for (topic in topics) subscriptions.remove(topic)?.evict(cause)
            applySubscriptions()
        }

        // Order matters: close before drain (no new commands can land mid-drain),
        // drain before evict (queued unregister continuations need resuming before
        // subscribers wake into their finally and find the channel closed).
        private fun cleanupOnFailure(cause: Throwable) {
            commandCh.close()
            while (true) {
                val cmd = commandCh.tryReceive().getOrNull() ?: break
                when (cmd) {
                    is GroupCommand.Register -> cmd.subscription.evict(cause)

                    is GroupCommand.Unregister -> cmd.cont.resume(Unit)
                    is GroupCommand.TransitionComplete, is GroupCommand.TransitionFailed -> {}
                }
            }
            try {
                evictSubscriptions(subscriptions.keys.toList(), cause)
            } catch (cleanup: Throwable) {
                LOG.warn(cleanup, "error during subscription cleanup after poll loop failure")
            }
        }

        private suspend fun KafkaConsumer<*, ByteArray>.pollRecords() =
            runInterruptible(Dispatchers.IO) {
                try {
                    poll(pollDuration)
                } catch (_: WakeupException) {
                    ConsumerRecords.empty()
                } catch (e: InterruptException) {
                    throw InterruptedException().initCause(e)
                }
            }

        private val pollingJob: Job = scope.launch {
            try {
                var closed = false
                while (!closed) {
                    select {
                        commandCh.onReceiveCatching { result ->
                            result
                                .onSuccess { processCommand(it) }
                                .onClosed { closed = true }
                        }

                        if (subscriptions.isNotEmpty()) {
                            @OptIn(ExperimentalCoroutinesApi::class)
                            onTimeout(0.milliseconds) {
                                consumer.pollRecords()?.let { consumerRecords ->
                                    for ((topic, recs) in consumerRecords.groupBy { it.topic() }) {
                                        val sub = checkNotNull(subscriptions[topic]) {
                                            "Received records for unsubscribed topic $topic"
                                        }

                                        sub.processRecords(recs)
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (e: CancellationException) {
                cleanupOnFailure(e)
                throw e
            } catch (e: Throwable) {
                LOG.error(e) { "SharedGroupConsumer poll loop failed" }
                cleanupOnFailure(e)
                // Don't rethrow — already handled; the scope has no handler, so it'd just double-log.
            }
        }

        suspend fun <M> register(
            topic: String,
            codec: MessageCodec<M>,
            epoch: Int,
            listener: Log.SubscriptionListener<M>,
        ): TopicSubscription<M> {
            val sub = TopicSubscription(codec, epoch, listener)
            commandCh.send(GroupCommand.Register(topic, sub))
            consumer.wakeup()
            return sub
        }

        suspend fun unregister(topic: String) {
            suspendCancellableCoroutine { cont ->
                val result = commandCh.trySend(GroupCommand.Unregister(topic, cont))
                when {
                    // consumer shut down; subscription already evicted via cleanupOnFailure
                    result.isClosed -> cont.resume(Unit)
                    result.isSuccess -> consumer.wakeup()
                    else -> error("commandCh trySend failed: $result")
                }
            }
        }

        override fun close() {
            consumer.wakeup()
            // closing commandCh is the shutdown signal; do not also cancel pollingJob —
            // that races the select between observing closed-channel and observing cancellation
            commandCh.close()
            try {
                runBlocking { withTimeout(30.seconds) { pollingJob.join() } }
            } finally {
                check(subscriptions.isEmpty()) { "SharedGroupConsumer closed with active subscriptions: ${subscriptions.keys}" }
                consumer.close()
            }
        }
    }

    private val _sharedGroupConsumer = lazy { SharedGroupConsumer() }
    private val sharedGroupConsumer by _sharedGroupConsumer

    override fun close() {
        try {
            _sharedGroupConsumer.close()
        } finally {
            runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
            producer.close()
        }
    }

    @Serializable
    @SerialName("!Kafka")
    data class ClusterFactory @JvmOverloads constructor(
        val bootstrapServers: String,
        var pollDuration: Duration = Duration.ofSeconds(1),
        var propertiesMap: Map<String, String> = emptyMap(),
        var propertiesFile: Path? = null,
        var schemaRegistryUrl: String? = null,
        var transactionalIdPrefix: String? = null,
        var groupId: String = "xtdb",
        @kotlinx.serialization.Transient var coroutineContext: CoroutineContext = Dispatchers.Default
    ) : Remote.Factory<KafkaCluster> {

        fun pollDuration(pollDuration: Duration) = apply { this.pollDuration = pollDuration }
        fun propertiesMap(propertiesMap: Map<String, String>) = apply { this.propertiesMap = propertiesMap }
        fun propertiesFile(propertiesFile: Path) = apply { this.propertiesFile = propertiesFile }
        fun schemaRegistryUrl(schemaRegistryUrl: String) = apply { this.schemaRegistryUrl = schemaRegistryUrl }
        fun transactionalIdPrefix(transactionalIdPrefix: String?) =
            apply { this.transactionalIdPrefix = transactionalIdPrefix }

        fun groupId(groupId: String) = apply { this.groupId = groupId }

        private val Path.asPropertiesMap: Map<String, String>
            get() =
                Properties()
                    .apply { load(inputStream()) }
                    .entries.associate { it.key as String to it.value as String }

        private val configMap: Map<String, String>
            get() = mapOf("bootstrap.servers" to bootstrapServers)
                .plus(propertiesMap)
                .plus(propertiesFile?.asPropertiesMap.orEmpty())

        override fun open(): KafkaCluster =
            KafkaCluster(
                configMap, pollDuration, schemaRegistryUrl,
                // normalise empty/blank prefix to null so `ENV XTDB_TRANSACTIONAL_ID_PREFIX=""`
                // from the Docker image produces `xtdb-leader` rather than `-xtdb-leader`.
                transactionalIdPrefix?.ifBlank { null },
                groupId, coroutineContext
            )
    }

    /** @suppress */
    interface AtomicProducer<M> : Log.AtomicProducer<M> {
        override fun openTx(): Tx<M>

        interface Tx<M> : Log.AtomicProducer.Tx<M> {
            fun sendOffsetsToTransaction(
                offsets: Map<TopicPartition, OffsetAndMetadata>,
                groupMetadata: ConsumerGroupMetadata,
            )
        }
    }

    private inner class KafkaLog<M>(
        private val codec: MessageCodec<M>,
        private val topic: String,
        override val epoch: Int,
    ) : Log<M> {

        private fun readLatestSubmittedMessage(kafkaConfigMap: KafkaConfigMap): LogOffset =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                (c.endOffsets(listOf(tp))[tp] ?: 0) - 1
            }

        private val latestSubmittedOffset0 = AtomicLong(readLatestSubmittedMessage(kafkaConfigMap))
        override val latestSubmittedOffset get() = latestSubmittedOffset0.get()

        override suspend fun appendMessage(message: M): Log.MessageMetadata =
            CompletableDeferred<Log.MessageMetadata>()
                .also { res ->
                    producer.send(
                        ProducerRecord(topic, null, Unit, codec.encode(message))
                    ) { recordMetadata, e ->
                        if (e == null) {
                            val metadata = Log.MessageMetadata(
                                epoch,
                                recordMetadata.offset(),
                                ofEpochMilli(recordMetadata.timestamp())
                            )
                            latestSubmittedOffset0.updateAndGet { it.coerceAtLeast(metadata.logOffset) }
                            res.complete(metadata)
                        } else res.completeExceptionally(e)
                    }
                }
                .await()

        override fun readLastMessage(): M? =
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                val lastOffset = c.endOffsets(listOf(tp))[tp]?.minus(1)?.takeIf { it >= 0 } ?: return null
                c.assign(listOf(tp))
                c.seek(tp, lastOffset)

                val records = c.poll(pollDuration).records(topic)
                records.firstOrNull()?.let { record -> codec.decode(record.value()) }
            }

        override fun readRecords(fromMsgId: MessageId, toMsgId: MessageId) = sequence {
            if (msgIdToEpoch(fromMsgId) != epoch || msgIdToEpoch(toMsgId) != epoch) return@sequence
            val fromOffset = msgIdToOffset(fromMsgId)
            val toOffset = msgIdToOffset(toMsgId)
            if (fromOffset >= toOffset) return@sequence

            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                c.assign(listOf(tp))

                val endOffset = c.endOffsets(listOf(tp))[tp] ?: 0
                val effectiveToOffset = minOf(toOffset, endOffset)
                if (fromOffset >= effectiveToOffset) return@sequence

                c.seek(tp, fromOffset)

                while (c.position(tp) < effectiveToOffset) {
                    for (rec in c.poll(pollDuration).records(topic)) {
                        if (rec.offset() >= effectiveToOffset) return@sequence
                        val msg = codec.decode(rec.value()) ?: continue
                        yield(Log.Record(epoch, rec.offset(), ofEpochMilli(rec.timestamp()), msg))
                    }
                }
            }
        }

        override fun openAtomicProducer(transactionalId: String) = object : AtomicProducer<M> {
            private val prefixedTxId = listOfNotNull(transactionalIdPrefix, transactionalId).joinToString("-")

            init {
                LOG.info { "starting atomic producer with transactional.id '$prefixedTxId'" }
            }

            private val producer = KafkaProducer(
                mapOf(
                    "enable.idempotence" to "true",
                    "acks" to "all",
                    "compression.type" to "snappy",
                    "linger.ms" to "0",
                    "transactional.id" to prefixedTxId,
                ) + kafkaConfigMap,
                UnitSerializer,
                ByteArraySerializer()
            ).also { it.initTransactions() }

            override fun openTx(): AtomicProducer.Tx<M> {
                producer.beginTransaction()

                return object : AtomicProducer.Tx<M> {
                    private val futures = mutableListOf<CompletableDeferred<Log.MessageMetadata>>()
                    private var isOpen = true

                    override fun appendMessage(message: M): CompletableDeferred<Log.MessageMetadata> {
                        check(isOpen) { "Transaction already closed" }
                        val deferred = CompletableDeferred<Log.MessageMetadata>()
                        futures.add(deferred)
                        producer.send(ProducerRecord(topic, null, Unit, codec.encode(message))) { recordMetadata, e ->
                            if (e == null) {
                                deferred.complete(
                                    Log.MessageMetadata(
                                        epoch,
                                        recordMetadata.offset(),
                                        ofEpochMilli(recordMetadata.timestamp())
                                    )
                                )
                            } else {
                                deferred.completeExceptionally(e)
                            }
                        }
                        return deferred
                    }

                    @OptIn(ExperimentalCoroutinesApi::class)
                    override fun commit() {
                        check(isOpen) { "Transaction already closed" }
                        isOpen = false
                        // commitTransaction flushes all pending sends, so deferreds are already complete
                        producer.commitTransaction()
                        futures.forEach {
                            latestSubmittedOffset0.updateAndGet { prev -> prev.coerceAtLeast(it.getCompleted().logOffset) }
                        }
                    }

                    override fun sendOffsetsToTransaction(
                        offsets: Map<TopicPartition, OffsetAndMetadata>,
                        groupMetadata: ConsumerGroupMetadata,
                    ) {
                        check(isOpen) { "Transaction already closed" }
                        producer.sendOffsetsToTransaction(offsets, groupMetadata)
                    }

                    override fun abort() {
                        check(isOpen) { "Transaction already closed" }
                        isOpen = false
                        producer.abortTransaction()
                    }

                    override fun close() {
                        if (isOpen) abort()
                    }
                }
            }

            override fun close() {
                producer.close()
            }
        }

        private fun KafkaConsumer<*, ByteArray>.pollRecords(): List<Log.Record<M>> =
            try {
                poll(pollDuration)
                    .records(topic)
                    .mapNotNull { rec ->
                        Log.Record(
                            epoch, rec.offset(), ofEpochMilli(rec.timestamp()),
                            codec.decode(rec.value()) ?: return@mapNotNull null
                        )
                    }
            } catch (_: WakeupException) {
                emptyList()
            } catch (e: InterruptException) {
                throw InterruptedException().initCause(e)
            }

        override suspend fun tailAll(afterMsgId: MessageId, processor: Log.RecordProcessor<M>) = coroutineScope {
            kafkaConfigMap.openConsumer().use { c ->
                val tp = TopicPartition(topic, 0)
                c.assign(listOf(tp))
                c.seekToAfterMsgId(tp, epoch, afterMsgId)

                while (isActive) {
                    val records = runInterruptible(Dispatchers.IO) { c.pollRecords() }
                    if (records.isNotEmpty()) processor.processRecords(records)
                }
            }
        }

        override suspend fun openGroupSubscription(listener: Log.SubscriptionListener<M>) {
            val sub = sharedGroupConsumer.register(topic, codec, epoch, listener)
            LOG.info { "registered group subscription for topic '$topic'" }
            try {
                sub.await()
            } finally {
                withContext(NonCancellable) { sharedGroupConsumer.unregister(topic) }
            }
        }

        override fun close() = Unit
    }

    @Serializable
    @SerialName("!Kafka")
    data class LogFactory @JvmOverloads constructor(
        val cluster: RemoteAlias,
        val topic: String,
        var replicaCluster: RemoteAlias = cluster,
        var replicaTopic: String = "$topic-replica",
        var autoCreateTopic: Boolean = true,
        var epoch: Int = 0,
    ) : Log.Factory {

        fun replicaCluster(replicaCluster: RemoteAlias) = apply { this.replicaCluster = replicaCluster }
        fun replicaTopic(replicaTopic: String) = apply { this.replicaTopic = replicaTopic }
        fun autoCreateTopic(autoCreateTopic: Boolean) = apply { this.autoCreateTopic = autoCreateTopic }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>): Log<SourceMessage> {
            val clusterAlias = this.cluster
            val cluster = requireNotNull(remotes[clusterAlias] as? KafkaCluster) {
                "missing Kafka cluster: '$clusterAlias'"
            }

            val configMap = cluster.kafkaConfigMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(topic, autoCreateTopic)
            }

            return cluster.KafkaLog(SourceMessage.Codec, topic, epoch)
        }

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>) =
            ReadOnlyLog(openSourceLog(remotes))

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>): Log<ReplicaMessage> {
            val clusterAlias = this.replicaCluster
            val cluster = requireNotNull(remotes[clusterAlias] as? KafkaCluster) {
                "missing Kafka cluster: '$clusterAlias'"
            }

            val configMap = cluster.kafkaConfigMap

            AdminClient.create(configMap).use { admin ->
                admin.ensureTopicExists(replicaTopic, autoCreateTopic)
            }

            return cluster.KafkaLog(ReplicaMessage.Codec, replicaTopic, epoch)
        }

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>) =
            ReadOnlyLog(openReplicaLog(remotes))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.setOtherLog(ProtoAny.pack(kafkaLogConfig {
                this.topic = this@LogFactory.topic
                this.epoch = this@LogFactory.epoch
                this.logClusterAlias = cluster
                this.replicaClusterAlias = replicaCluster
                this.replicaTopic = this@LogFactory.replicaTopic
            }, "proto.xtdb.com"))
        }
    }

    /**
     * @suppress
     */
    class Registration : Log.Registration {
        override val protoTag: String get() = "proto.xtdb.com/xtdb.kafka.proto.KafkaLogConfig"

        override fun fromProto(msg: ProtoAny) =
            msg.unpack(KafkaLogConfig::class.java).let {
                LogFactory(it.logClusterAlias, it.topic).apply {
                    epoch = it.epoch
                    if (it.hasReplicaClusterAlias()) replicaCluster = it.replicaClusterAlias
                    if (it.hasReplicaTopic()) replicaTopic = it.replicaTopic
                }
            }

        override fun registerSerde(builder: PolymorphicModuleBuilder<Log.Factory>) {
            builder.subclass(LogFactory::class)
        }
    }

    /**
     * @suppress
     */
    class ClusterRegistration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(ClusterFactory::class)
        }
    }
}
