package xtdb.compactor

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.time.withTimeout
import xtdb.api.log.SourceMessage.TriesAdded
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.compactor.PageTree.Companion.asTree
import org.apache.arrow.memory.BufferAllocator
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.error.Fault
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.TrieMetadata
import xtdb.segment.BufferPoolSegment
import xtdb.api.TableRef
import xtdb.trie.*
import xtdb.util.*
import java.nio.channels.ClosedByInterruptException
import java.time.Duration
import kotlin.use

typealias JobKey = Pair<TableRef, TrieKey>

// a failed entry is kept (unlike a successful one, which is dropped) so we don't retry a poison
// job and `compactAll` can surface its error.
private sealed interface JobState {
    data object InFlight : JobState
    data class Failed(val error: Throwable) : JobState
}

private val LOGGER = Compactor::class.logger

interface Compactor : AutoCloseable {

    interface Job {
        val table: TableRef
        val trieKeys: List<TrieKey>
        val part: ByteArray
        val outputTrieKey: Trie.Key
        val partitionedByRecency: Boolean
    }

    interface JobCalculator {
        fun availableJobs(trieCatalog: TrieCatalog): Collection<Job>
    }

    fun interface Factory {
        fun create(meterRegistry: MeterRegistry?, threads: Int): Compactor
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

    fun openForDatabase(scope: CoroutineScope, allocator: BufferAllocator, dbStorage: DatabaseStorage, dbState: DatabaseState, watchers: Watchers): ForDatabase

    interface Driver : AutoCloseable {
        suspend fun executeJob(job: Job): TriesAdded
        suspend fun publishTries(triesAdded: TriesAdded)

        interface Factory {
            fun create(allocator: BufferAllocator, dbStorage: DatabaseStorage, dbState: DatabaseState, watchers: Watchers): Driver
        }

        companion object {
            @JvmStatic
            fun real(pageSize: Int, recencyPartition: RecencyPartition?) =
                object : Factory {
                    override fun create(allocator: BufferAllocator, dbStorage: DatabaseStorage, dbState: DatabaseState, watchers: Watchers) = object : Driver {
                        private val al = allocator.openChildAllocator("compactor")
                        private val log = dbStorage.sourceLog
                        private val bp = dbStorage.bufferPool
                        private val mm = dbStorage.metadataManager

                        private val trieWriter = PageTrieWriter(al, bp, calculateBlooms = true)
                        private val segMerge = SegmentMerge(al)

                        private fun Job.trieDetails(
                            trieKey: TrieKey,
                            dataFileSize: FileSize,
                            trieMetadata: TrieMetadata?
                        ) =
                            TrieDetails.newBuilder()
                                .setTableName(table.sym.toString())
                                .setTrieKey(trieKey)
                                .setDataFileSize(dataFileSize)
                                .setTrieMetadata(trieMetadata)
                                .build()

                        override suspend fun executeJob(job: Job): TriesAdded =
                            try {
                                job.trieKeys.map { BufferPoolSegment(al, bp, mm, job.table, it) }.useAll { segs ->
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
                                                                    job.table, trieKey,
                                                                    loader, it.leaves.asTree,
                                                                    pageSize
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
        private val dispatcher: CoroutineDispatcher = Dispatchers.Default
    ) : Compactor {

        private val jobsDispatcher = dispatcher.limitedParallelism(threadCount, "compactor")
        private val jobsSemaphore = Semaphore(threadCount)

        override fun openForDatabase(scope: CoroutineScope, allocator: BufferAllocator, dbStorage: DatabaseStorage, dbState: DatabaseState, watchers: Watchers) = object : ForDatabase {

            private val trieCatalog = dbState.trieCatalog
            private val liveIndex = dbState.liveIndexOrNull

            val driver = driverFactory.create(allocator, dbStorage, dbState, watchers)

            @Volatile
            private var availableJobs = emptyMap<JobKey, Job>()

            // started jobs by disposition; touched only by the outer-loop coroutine, so unsynchronised.
            private val jobStates = mutableMapOf<JobKey, JobState>()

            private val jobTimer: Timer? = meterRegistry?.let {
                Timer.builder("compactor.job.timer")
                    .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
                    .register(it)
            }

            private val doneCh = Channel<Pair<JobKey, Throwable?>>()
            private val wakeupCh = Channel<Unit>(1, onBufferOverflow = DROP_OLDEST)
            private var compactAllPromise: CompletableDeferred<Unit>? = null

            init {
                meterRegistry?.let {
                    Gauge.builder("compactor.jobs.available") { jobCalculator.availableJobs(trieCatalog).size.toDouble() }
                        .register(it)
                }
            }

            init {
                scope.launch(dispatcher + CoroutineName("outer loop")) {
                    // supervisorScope so a failed job doesn't cancel the loop or its sibling jobs.
                    supervisorScope {
                        while (true) {
                            availableJobs =
                                jobCalculator.availableJobs(trieCatalog)
                                    .associateBy { JobKey(it.table, it.outputTrieKey.toString()) }

                            // excluding started jobs is what stops a still-running (or failed) job
                            // being re-launched.
                            val pendingJobs = availableJobs.keys.filterNot { it in jobStates }

                            if (pendingJobs.isEmpty() && jobStates.values.none { it is JobState.InFlight }) {
                                LOGGER.trace("sending idle")

                                val failed = jobStates.mapNotNull { (jobKey, state) ->
                                    (state as? JobState.Failed)?.let { jobKey to it.error }
                                }

                                if (failed.isEmpty())
                                    compactAllPromise?.complete(Unit)
                                else
                                    // surface *every* failed job rather than completing normally —
                                    // otherwise the caller can't tell a drained compactor from one
                                    // stuck on a failure, and can't see which job(s) failed. first as
                                    // cause, rest suppressed, so a caller can find the one it induced.
                                    compactAllPromise?.completeExceptionally(
                                        Fault("compaction failed: ${failed.joinToString { (k, _) -> "${k.first}/${k.second}" }}",
                                              "xtdb.compactor/jobs-failed", cause = failed.first().second)
                                            .apply { failed.drop(1).forEach { (_, e) -> addSuppressed(e) } }
                                    )
                            }

                            pendingJobs.forEach { jobKey ->
                                jobStates[jobKey] = JobState.InFlight
                                launch(jobsDispatcher + CoroutineName("job: ${jobKey.first} / ${jobKey.second}")) {
                                    val error = try {
                                        jobsSemaphore.withPermit {
                                            // check it's still required
                                            val job = availableJobs[jobKey]
                                            if (job != null) {
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
                                        }
                                        null
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Throwable) {
                                        // already logged by the driver; kept to surface via compactAll
                                        e
                                    }

                                    doneCh.send(jobKey to error)
                                }
                            }

                            select {
                                doneCh.onReceive { (jobKey, error) ->
                                    if (error != null) jobStates[jobKey] = JobState.Failed(error)
                                    else jobStates.remove(jobKey)
                                }

                                wakeupCh.onReceive {
                                    LOGGER.trace("wakey wakey")
                                }
                            }
                        }
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

                // Compaction lands `TriesAdded` on the trie-cat asynchronously, so the live-index's
                // cached `sharedSnap` would otherwise stay frozen at the last commit/`nextBlock`.
                // Callers waiting for `compactAll` are explicitly synchronising on the post-compaction
                // catalog, so refresh the snap here rather than at every test/dev call-site.
                liveIndex?.refreshSnap()
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
            override fun openForDatabase(scope: CoroutineScope, allocator: BufferAllocator, dbStorage: DatabaseStorage, dbState: DatabaseState, watchers: Watchers) = object : ForDatabase {
                override fun signalBlock() = Unit
                override suspend fun compactAll() = Unit
                override fun close() = Unit
            }

            override fun close() = Unit
        }
    }
}
