package xtdb.compactor

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.time.withTimeout
import org.apache.arrow.memory.BufferAllocator
import xtdb.BufferPool
import xtdb.api.log.Log
import xtdb.api.log.Log.Message.TriesAdded
import xtdb.arrow.Relation
import xtdb.compactor.PageTree.Companion.asTree
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.TrieMetadata
import xtdb.metadata.PageMetadata
import xtdb.trie.*
import xtdb.trie.ISegment.Segment
import xtdb.trie.Trie.metaFilePath
import xtdb.util.*
import java.nio.channels.ClosedByInterruptException
import java.time.Duration
import kotlin.time.Duration.Companion.seconds

private typealias JobKey = Pair<TableName, TrieKey>

interface Compactor : AutoCloseable {

    interface Job {
        val tableName: String
        val trieKeys: Set<TrieKey>
        val part: ByteArray
        val outputTrieKey: TrieKey
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
        threadCount: Int, private val pageSize: Int,
    ) : Compactor {
        private val al = al.openChildAllocator("compactor")
            .also { meterRegistry?.register(it) }

        private val trieWriter = TrieWriter(al, bp, true)


        private val segMerge = SegmentMerge(al)

        companion object {
            private val LOGGER = Compactor::class.logger

        }

        private fun Job.trieDetails(dataFileSize: FileSize, trieMetadata: TrieMetadata) =
            TrieDetails.newBuilder()
                .setTableName(tableName).setTrieKey(outputTrieKey)
                .setDataFileSize(dataFileSize)
                .setTrieMetadata(trieMetadata)
                .build()

        private fun Job.execute(): List<TrieDetails> =
            try {
                LOGGER.debug("compacting '$tableName' '$trieKeys' -> $outputTrieKey")

                DataRel.openRels(al, bp, tableName, trieKeys).useAll { dataRels ->
                    mutableListOf<PageMetadata>().useAll { pageMetadatas ->
                        for (trieKey in trieKeys) {
                            pageMetadatas.add(mm.openPageMetadata(tableName.metaFilePath(trieKey)))
                        }

                        val segments = (pageMetadatas zip dataRels)
                            .map { (pageMetadata, dataRel) -> Segment(pageMetadata.trie, dataRel) }

                        useTempFile("merged-segments", ".arrow") { tempFile ->
                            val pageTree =
                                with(segMerge) { segments.mergeTo(tempFile.openWritableChannel(), part).asTree }

                            Relation.loader(al, tempFile).use { loader ->
                                val (dataFileSize, trieMetadata )=
                                    trieWriter.writePageTree(tableName, outputTrieKey, loader, pageTree, pageSize)

                                LOGGER.debug("compacted '$tableName' -> '$outputTrieKey'")

                                listOf(trieDetails(dataFileSize, trieMetadata))
                            }
                        }
                    }
                }
            } catch (e: ClosedByInterruptException) {
                throw InterruptedException(e.message)
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Throwable) {
                LOGGER.error(e) { "error running compaction job: $tableName/$outputTrieKey" }
                throw e
            }

        private val scope = CoroutineScope(Dispatchers.Default)

        private val jobsScope =
            CoroutineScope(
                Dispatchers.Default.limitedParallelism(threadCount, "compactor")
                        + SupervisorJob(scope.coroutineContext.job)
            )

        private val wakeup = Channel<Unit>(1, onBufferOverflow = DROP_OLDEST)
        private val idle = Channel<Unit>()

        @Volatile
        private var availableJobs = emptyMap<JobKey, Job>()

        private val queuedJobs = mutableSetOf<JobKey>()

        init {
            scope.launch {
                val doneCh = Channel<JobKey>()

                while (true) {
                    availableJobs = jobCalculator.availableJobs().associateBy { JobKey(it.tableName, it.outputTrieKey) }

                    if (availableJobs.isEmpty() && queuedJobs.isEmpty()) {
                        LOGGER.trace("sending idle")
                        idle.trySend(Unit)
                    }

                    availableJobs.keys.forEach { jobKey ->
                        if (queuedJobs.add(jobKey)) {
                            jobsScope.launch {
                                // check it's still required
                                val job = availableJobs[jobKey]
                                if (job != null) {
                                    val addedTries = runInterruptible { job.execute() }

                                    // add the trie to the catalog eagerly so that it's present
                                    // next time we run `availableJobs` (it's idempotent)
                                    trieCatalog.addTries(addedTries)
                                    log.appendMessage(TriesAdded(addedTries)).await()
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
            val job = scope.launch {
                LOGGER.trace("compactAll: waiting for idle")
                if (timeout == null) idle.receive() else withTimeout(timeout) { idle.receive() }
                LOGGER.trace("compactAll: idle")
            }

            wakeup.trySend(Unit)

            runBlocking { job.join() }
        }

        override fun close() {
            runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
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
