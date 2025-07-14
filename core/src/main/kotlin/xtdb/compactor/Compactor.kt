package xtdb.compactor

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.time.withTimeout
import org.apache.arrow.memory.BufferAllocator
import xtdb.BufferPool
import xtdb.api.log.Log
import xtdb.api.log.Log.Message.TriesAdded
import xtdb.api.storage.Storage
import xtdb.arrow.Relation
import xtdb.compactor.PageTree.Companion.asTree
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.TrieMetadata
import xtdb.metadata.PageMetadata
import xtdb.table.TableRef
import xtdb.trie.*
import xtdb.trie.ISegment.Segment
import xtdb.trie.Trie.metaFilePath
import xtdb.util.*
import java.nio.channels.ClosedByInterruptException
import java.time.Duration

private typealias JobKey = Pair<TableRef, TrieKey>

interface Compactor : AutoCloseable {

    interface Job {
        val table: TableRef
        val trieKeys: List<TrieKey>
        val part: ByteArray
        val outputTrieKey: Trie.Key
        val partitionedByRecency: Boolean
    }

    interface JobCalculator {
        fun availableJobs(): Collection<Job>
    }

    fun signalBlock()
    fun compactAll(timeout: Duration? = null)

    class Impl(
        al: BufferAllocator, private val bp: BufferPool, private val mm: PageMetadata.Factory,
        private val log: Log, private val trieCatalog: TrieCatalog, meterRegistry: MeterRegistry?,
        private val jobCalculator: JobCalculator,
        private val ignoreBlockSignal: Boolean,
        private val pool: CompactionPool,
        private val pageSize: Int,
        private val recencyPartition: RecencyPartition?
    ) : Compactor {
        private val al = al.openChildAllocator("compactor")
            .also { meterRegistry?.register(it) }

        private val trieWriter = TrieWriter(al, bp, calculateBlooms = true)
        private val segMerge = SegmentMerge(al)

        companion object {
            private val LOGGER = Compactor::class.logger

        }

        private fun Job.trieDetails(trieKey: TrieKey, dataFileSize: FileSize, trieMetadata: TrieMetadata?) =
            TrieDetails.newBuilder()
                .setTableName(table.sym.toString())
                .setTrieKey(trieKey)
                .setDataFileSize(dataFileSize)
                .setTrieMetadata(trieMetadata)
                .build()

        private fun Job.execute(): List<TrieDetails> =
            try {
                LOGGER.debug("compacting '${table.sym}' '$trieKeys' -> $outputTrieKey")

                DataRel.openRels(al, bp, table, trieKeys).useAll { dataRels ->
                    mutableListOf<PageMetadata>().useAll { pageMetadatas ->
                        for (trieKey in trieKeys) {
                            pageMetadatas.add(mm.openPageMetadata(table.metaFilePath(trieKey)))
                        }

                        val segments = (pageMetadatas zip dataRels)
                            .map { (pageMetadata, dataRel) -> Segment(pageMetadata.trie, dataRel) }

                        val recencyPartitioning =
                            if (partitionedByRecency) SegmentMerge.RecencyPartitioning.Partition
                            else SegmentMerge.RecencyPartitioning.Preserve(outputTrieKey.recency)

                        segMerge.mergeSegments(segments, part, recencyPartitioning, this@Impl.recencyPartition)
                            .useAll { mergeRes ->
                                mergeRes.map {
                                    it.openForRead().use { mergeReadCh ->
                                        Relation.loader(al, mergeReadCh).use { loader ->
                                            val trieKey = outputTrieKey.copy(recency = it.recency).toString()

                                            val (dataFileSize, trieMetadata) =
                                                trieWriter.writePageTree(
                                                    table, trieKey,
                                                    loader, it.leaves.asTree,
                                                    pageSize
                                                )

                                            LOGGER.debug("compacted '${table.sym}' -> '$outputTrieKey'")

                                            trieDetails(trieKey, dataFileSize, trieMetadata)
                                        }
                                    }
                                }
                            }
                    }
                }
            } catch (e: ClosedByInterruptException) {
                throw InterruptedException(e.message)
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Throwable) {
                LOGGER.error(e) { "error running compaction job: ${table.sym}/$outputTrieKey" }
                throw e
            }

        private val wakeup = Channel<Unit>(1, onBufferOverflow = DROP_OLDEST)
        private val idle = Channel<Unit>()

        @Volatile
        private var availableJobs = emptyMap<JobKey, Job>()

        private val queuedJobs = mutableSetOf<JobKey>()
        private val jobTimer: Timer? = meterRegistry?.let {
            Timer.builder("compactor.job.timer")
                .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
                .register(it)
        }

        init {

            meterRegistry?.let {
                Gauge.builder("compactor.jobs.available") { jobCalculator.availableJobs().size.toDouble() }
                    .register(it)
            }

            pool.scope.launch {
                val doneCh = Channel<JobKey>()

                while (true) {
                    availableJobs =
                        jobCalculator.availableJobs().associateBy { JobKey(it.table, it.outputTrieKey.toString()) }

                    if (availableJobs.isEmpty() && queuedJobs.isEmpty()) {
                        LOGGER.trace("sending idle")
                        idle.trySend(Unit)
                    }

                    availableJobs.keys.forEach { jobKey ->
                        if (queuedJobs.add(jobKey)) {
                            pool.jobsScope.launch {
                                // check it's still required
                                val job = availableJobs[jobKey]
                                if (job != null) {
                                    val timer = meterRegistry?.let { Timer.start(it) }
                                    val addedTries = runInterruptible { job.execute() }
                                    jobTimer?.let { timer?.stop(it) }
                                    val messageMetadata =
                                        log.appendMessage(TriesAdded(Storage.VERSION, addedTries)).await()
                                    // add the trie to the catalog eagerly so that it's present
                                    // next time we run `availableJobs` (it's idempotent)
                                    trieCatalog.addTries(
                                        job.table,
                                        addedTries,
                                        messageMetadata.logTimestamp
                                    )

                                }

                                doneCh.send(jobKey)
                            }
                        }
                    }

                    select {
                        doneCh.onReceive {
                            queuedJobs.remove(it)
                        }

                        wakeup.onReceive {
                            LOGGER.trace("wakey wakey")
                        }
                    }
                }
            }
        }

        override fun signalBlock() {
            if (!ignoreBlockSignal) wakeup.trySend(Unit)
        }

        override fun compactAll(timeout: Duration?) {
            val job = pool.scope.launch {
                LOGGER.trace("compactAll: waiting for idle")
                if (timeout == null) idle.receive() else withTimeout(timeout) { idle.receive() }
                LOGGER.trace("compactAll: idle")
            }

            wakeup.trySend(Unit)

            runBlocking { job.join() }
        }

        override fun close() {
            segMerge.close()

            LOGGER.debug("compactor closed")
        }
    }

    companion object {
        @JvmField
        val NOOP = object : Compactor {
            override fun signalBlock() {}
            override fun compactAll(timeout: Duration?) {}
            override fun close() {}
        }
    }
}
