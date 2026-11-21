package xtdb.compactor

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.time.withTimeout
import xtdb.api.CompactorConfig
import xtdb.api.log.SourceMessage.IndexBackfilled
import xtdb.api.log.SourceMessage.TriesAdded
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import org.apache.arrow.memory.BufferAllocator
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.index.IndexCatalog
import xtdb.index.IndexDef
import xtdb.index.IndexState
import xtdb.index.IndexTrieWriter
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.TrieMetadata
import xtdb.segment.BufferPoolSegment
import xtdb.api.TableRef
import xtdb.table.IndexFiles
import xtdb.trie.*
import xtdb.util.*
import java.nio.channels.ClosedByInterruptException
import java.time.Duration
import java.util.UUID
import kotlin.use

private val LOGGER = Compactor::class.logger

private sealed interface QueueKey {
    data class Data(val table: TableRef, val outputTrieKey: TrieKey) : QueueKey
    data class Backfill(val indexId: UUID, val block: BlockIndex) : QueueKey
}

private val IndexCatalog.backfillJobs: Map<QueueKey.Backfill, IndexDef>
    get() = indices
        .flatMap { index ->
            (index.state as? IndexState.Building)?.owedBlocks.orEmpty().map { QueueKey.Backfill(index.def.id, it) to index.def }
        }
        .toMap()

private sealed interface QueuedJob {
    data object Running : QueuedJob
    class Failed(val cause: Throwable) : QueuedJob
}

interface Compactor : AutoCloseable {

    interface Job {
        val table: TableRef
        val trieKeys: List<TrieKey>
        val part: ByteArray?
        val outputTrieKey: Trie.Key
        val partitionedByRecency: Boolean
    }

    interface JobCalculator {
        fun availableJobs(trieCatState: TrieCatalog.State): Collection<Job>
    }

    fun interface Factory {
        fun create(meterRegistry: MeterRegistry?, config: CompactorConfig): Compactor
    }

    interface ForDatabase : AutoCloseable {
        fun signalBlock()
        suspend fun compactAll()

        fun compactAllSync(timeout: Duration?) {
            runBlocking {
                if (timeout == null) compactAll()
                else withTimeout(timeout) { compactAll() }
            }
        }
    }

    fun openForDatabase(scope: CoroutineScope, allocator: BufferAllocator, partitionStorage: PartitionStorage, partitionState: PartitionState, watchers: Watchers): ForDatabase

    interface Driver : AutoCloseable {
        suspend fun executeJob(job: Job): TriesAdded
        suspend fun publishTries(triesAdded: TriesAdded)

        /** Writes [index]'s L0 for [block], from the table's data L0 for that block. */
        suspend fun executeBackfill(index: IndexDef, block: BlockIndex): IndexBackfilled
        suspend fun publishBackfill(backfilled: IndexBackfilled)

        interface Factory {
            fun create(allocator: BufferAllocator, partitionStorage: PartitionStorage, partitionState: PartitionState, watchers: Watchers): Driver
        }

        companion object {
            // TODO(#6263): `writesRecency` goes with withinFileRecencyBranching
            @JvmStatic
            @JvmOverloads
            fun real(pageSize: Int, recencyPartition: RecencyPartition?, reeIids: Boolean = false, writesRecency: Boolean = false) =
                object : Factory {
                    override fun create(allocator: BufferAllocator, partitionStorage: PartitionStorage, partitionState: PartitionState, watchers: Watchers) = object : Driver {
                        private val al = allocator.openChildAllocator("compactor")
                        private val log = partitionStorage.sourceLog
                        private val bp = partitionStorage.bufferPool
                        private val mm = partitionStorage.metadataManager
                        private val tableCat = partitionState.tableCatalog

                        private val trieWriter = PageTrieWriter(al, bp, calculateBlooms = true)
                        private val segMerge = SegmentMerge(al, reeIids, writesRecency)
                        private val indexTrieWriter = IndexTrieWriter(al, bp)

                        private fun Job.trieDetails(
                            trieKey: TrieKey,
                            dataFileSize: FileSize,
                            trieMetadata: TrieMetadata
                        ) =
                            TrieDetails.newBuilder()
                                .setTableName(table.sym.toString())
                                .setTrieKey(trieKey)
                                .setDataFileSize(dataFileSize)
                                .setTrieMetadata(trieMetadata)
                                .build()

                        override suspend fun executeJob(job: Job): TriesAdded =
                            try {
                                val slug = tableCat.snap().slug(job.table)

                                job.trieKeys.map { BufferPoolSegment.open(al, bp, mm, slug, it) }.useAll { segs ->
                                    val recencyPartitioning =
                                        if (job.partitionedByRecency) SegmentMerge.RecencyPartitioning.Partition
                                        else SegmentMerge.RecencyPartitioning.Preserve(job.outputTrieKey.recency)

                                    val addedTries =
                                        segMerge.mergeSegments(segs, job.part, recencyPartitioning, recencyPartition)
                                            .use { mergeRes ->
                                                mergeRes.map {
                                                    it.openForRead().use { mergeReadCh ->
                                                        Relation.loader(al, mergeReadCh).use { loader ->
                                                            val trieKey =
                                                                job.outputTrieKey.copy(recency = it.recency).toString()

                                                            val (dataFileSize, trieMetadata) =
                                                                trieWriter.writePageTree(
                                                                    slug, trieKey,
                                                                    loader, it.leaves,
                                                                    pageSize, job.part
                                                                )

                                                            job.trieDetails(trieKey, dataFileSize, trieMetadata)
                                                        }
                                                    }
                                                }
                                            }

                                    TriesAdded(Storage.VERSION, bp.epoch, addedTries)
                                }
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: ClosedByInterruptException) {
                                throw InterruptedException(e.message)
                            } catch (e: InterruptedException) {
                                throw e
                            } catch (e: Throwable) {
                                LOGGER.error(e) { "error running compaction job: ${job.table.sym}/${job.outputTrieKey}, files in job: '${job.trieKeys}'" }
                                throw e
                            }

                        override suspend fun publishTries(triesAdded: TriesAdded) {
                            val msgId = log.appendMessage(triesAdded).msgId
                            watchers.awaitSource(msgId)
                        }

                        override suspend fun executeBackfill(index: IndexDef, block: BlockIndex): IndexBackfilled {
                            val files = IndexFiles(tableCat.snap().slug(index.table), index.id)
                            val l0 = indexTrieWriter.backfillL0(files, block, index.column)
                            return IndexBackfilled(Storage.VERSION, bp.epoch, index.id, block, l0?.dataFileSize)
                        }

                        override suspend fun publishBackfill(backfilled: IndexBackfilled) {
                            val msgId = log.appendMessage(backfilled).msgId
                            watchers.awaitSource(msgId)
                        }

                        override fun close() {
                            segMerge.close()
                            al.close()
                        }
                    }
                }

        }
    }

    class Impl @JvmOverloads constructor(
        private val driverFactory: Driver.Factory,
        private val meterRegistry: MeterRegistry?,
        private val jobCalculator: JobCalculator,
        private val ignoreSignalBlock: Boolean,
        threadCount: Int,
        private val dispatcher: CoroutineDispatcher = Dispatchers.Default,
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO
    ) : Compactor {

        // The IO in a job isn't a trailing write that could be hoisted out — segment reads, merge
        // spills, trie writes and the publishing append run throughout, interleaved with the merge's
        // CPU work too finely to separate — so the whole job goes on the IO pool, and only the
        // orchestration loop stays on [dispatcher].
        private val jobsDispatcher = ioDispatcher.limitedParallelism(threadCount, "compactor")
        private val jobsSemaphore = Semaphore(threadCount)

        override fun openForDatabase(scope: CoroutineScope, allocator: BufferAllocator, partitionStorage: PartitionStorage, partitionState: PartitionState, watchers: Watchers) = object : ForDatabase {

            private val trieCatalog = partitionState.trieCatalog
            private val indexCatalog = partitionState.indexCatalogFlow

            val driver = driverFactory.create(allocator, partitionStorage, partitionState, watchers)

            @Volatile
            private var availableJobs = emptyMap<QueueKey.Data, Job>()

            @Volatile
            private var availableBackfills = emptyMap<QueueKey.Backfill, IndexDef>()

            private val queuedJobs = mutableMapOf<QueueKey, QueuedJob>()

            private val jobTimer: Timer? = meterRegistry?.let {
                Timer.builder("compactor.job.timer")
                    .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
                    .register(it)
            }

            private val doneCh = Channel<Pair<QueueKey, Result<Unit>>>()
            private val wakeupCh = Channel<Unit>(1, onBufferOverflow = DROP_OLDEST)
            private var compactAllPromise: CompletableDeferred<Unit>? = null

            init {
                meterRegistry?.let {
                    Gauge.builder("compactor.jobs.available") { jobCalculator.availableJobs(trieCatalog.state).size.toDouble() }
                        .register(it)
                }
            }

            init {
                // a declaration, and a backfill applied, change the backfill jobs with no block to signal it
                scope.launch(dispatcher + CoroutineName("backfill jobs watcher")) {
                    indexCatalog.map { it.backfillJobs.keys }.distinctUntilChanged().collect { signalBlock() }
                }
            }

            init {
                scope.launch(dispatcher + CoroutineName("outer loop")) {
                    // supervisorScope so a failed job doesn't cancel the loop or its sibling jobs.
                    supervisorScope {
                        while (true) {
                            availableJobs =
                                jobCalculator.availableJobs(trieCatalog.state)
                                    .associateBy { QueueKey.Data(it.table, it.outputTrieKey.toString()) }

                            availableBackfills = indexCatalog.value.backfillJobs

                            (availableJobs.keys + availableBackfills.keys).forEach { jobKey ->
                                if (queuedJobs.putIfAbsent(jobKey, QueuedJob.Running) == null) {
                                    launch(jobsDispatcher + CoroutineName("job: $jobKey")) {
                                        val result = try {
                                            runJob(jobKey)
                                            Result.success(Unit)
                                        } catch (e: CancellationException) {
                                            throw e
                                        } catch (e: Throwable) {
                                            Result.failure(e)
                                        }

                                        doneCh.send(jobKey to result)
                                    }
                                }
                            }

                            if (queuedJobs.values.none { it == QueuedJob.Running }) {
                                val failure = queuedJobs.values.filterIsInstance<QueuedJob.Failed>().firstOrNull()

                                if (failure == null) {
                                    LOGGER.trace("sending idle")
                                    compactAllPromise?.complete(Unit)
                                } else {
                                    LOGGER.trace("sending failure")
                                    compactAllPromise?.completeExceptionally(failure.cause)
                                }
                            }

                            select {
                                doneCh.onReceive { (jobKey, result) ->
                                    result.fold(
                                        onSuccess = { queuedJobs.remove(jobKey) },
                                        // a failed job stays queued so that this node doesn't attempt it again
                                        onFailure = { queuedJobs[jobKey] = QueuedJob.Failed(it) }
                                    )
                                }

                                wakeupCh.onReceive {
                                    LOGGER.trace("wakey wakey")
                                }
                            }
                        }
                    }
                }
            }

            private suspend fun runJob(jobKey: QueueKey) {
                jobsSemaphore.withPermit {
                    when (jobKey) {
                        is QueueKey.Data -> runDataJob(jobKey)
                        is QueueKey.Backfill -> runBackfill(jobKey)
                    }
                }
            }

            private suspend fun runBackfill(jobKey: QueueKey.Backfill) {
                val index = availableBackfills[jobKey] ?: return

                LOGGER.debug("backfilling index '${index.name}' (${index.id}) for block ${jobKey.block}")
                driver.publishBackfill(driver.executeBackfill(index, jobKey.block))
            }

            private suspend fun runDataJob(jobKey: QueueKey.Data) {
                val job = availableJobs[jobKey] ?: return

                LOGGER.debug("compacting '${job.table.sym}' ${job.trieKeys} -> ${job.outputTrieKey}")

                val timer = meterRegistry?.let { Timer.start(it) }
                val triesAdded = driver.executeJob(job)
                jobTimer?.let { timer?.stop(it) }
                driver.publishTries(triesAdded)

                LOGGER.debug {
                    buildString {
                        append("compacted '${job.table.sym}'")
                        append(" -> ")
                        append(
                            triesAdded.tries
                                .joinToString(prefix = "(", postfix = ")") { it.trieKey })
                    }
                }
            }

            override fun signalBlock() {
                if (!ignoreSignalBlock) wakeupCh.trySend(Unit)
            }

            override suspend fun compactAll() {
                val promise = CompletableDeferred<Unit>().also { compactAllPromise = it }
                wakeupCh.send(Unit)
                LOGGER.trace("compactAll: waiting for idle")
                promise.await()
                LOGGER.trace("compactAll: idle")
            }

            override fun close() {
                driver.close()
                LOGGER.debug("compactor closed")
            }
        }
    }

    override fun close() = Unit

    companion object {
        @JvmField
        val NOOP = object : Compactor {
            override fun openForDatabase(scope: CoroutineScope, allocator: BufferAllocator, partitionStorage: PartitionStorage, partitionState: PartitionState, watchers: Watchers) = object : ForDatabase {
                override fun signalBlock() = Unit
                override suspend fun compactAll() = Unit
                override fun close() = Unit
            }

            override fun close() = Unit
        }
    }
}
