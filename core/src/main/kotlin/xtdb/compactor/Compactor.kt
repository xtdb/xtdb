package xtdb.compactor

import com.carrotsearch.hppc.IntArrayList
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.time.withTimeout
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.api.log.Log
import xtdb.api.log.Log.Message.TriesAdded
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.compactor.Compactor.Companion.RecencyGranularity.*
import xtdb.log.proto.AddedTrie
import xtdb.trie.FileSize
import xtdb.trie.HashTrie
import xtdb.trie.HashTrie.Companion.LEVEL_WIDTH
import xtdb.trie.TrieCatalog
import xtdb.trie.TrieWriter
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.trace
import java.time.Duration
import java.time.Instant
import java.time.LocalTime.MIDNIGHT
import java.time.OffsetDateTime
import java.time.YearMonth
import java.time.ZoneOffset.UTC
import java.util.*
import kotlin.math.ceil
import kotlin.time.Duration.Companion.seconds
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

private typealias InstantMicros = Long
private typealias Selection = IntArray

interface Compactor : AutoCloseable {

    interface Job {
        val tableName: String
        val outputTrieKey: String
    }

    interface Impl {
        fun availableJobs(): Collection<Job>
        fun executeJob(job: Job): AddedTrie
    }

    fun signalBlock()
    fun compactAll(timeout: Duration? = null)

    companion object {
        private val LOGGER = Compactor::class.logger

        internal fun Selection.partitionSlices(partIdxs: IntArray) =
            Array(LEVEL_WIDTH) { partition ->
                val cur = partIdxs[partition]
                val nxt = if (partition == partIdxs.lastIndex) size else partIdxs[partition + 1]

                if (cur == nxt) null else sliceArray(cur..<nxt)
            }

        internal fun Selection.iidPartitions(iidReader: VectorReader, level: Int): Array<Selection?> {
            val iidPtr = ArrowBufPointer()

            // for each partition, find the starting index in the selection
            val partIdxs = IntArray(LEVEL_WIDTH) { partition ->
                var left = 0
                var right = size
                var mid: Int
                while (left < right) {
                    mid = (left + right) / 2

                    val bucket = HashTrie.bucketFor(iidReader.getPointer(this[mid], iidPtr), level)

                    if (bucket < partition) left = mid + 1 else right = mid
                }

                left
            }

            // slice the selection array for each partition
            return partitionSlices(partIdxs)
        }

        internal enum class RecencyGranularity {
            YEAR, QUARTER, MONTH
        }

        private val YearMonth.startMicros get() = atDay(1).toEpochSecond(MIDNIGHT, UTC) * 1_000_000

        private fun InstantMicros.toDateTime() =
            OffsetDateTime.ofInstant(Instant.ofEpochSecond(this / 1_000_000, this % 1_000), UTC)

        private fun InstantMicros.recencyBucket(depth: RecencyGranularity): InstantMicros {
            // TODO will likely need some caching here.
            if (this == MAX_LONG) return MAX_LONG

            val ym = YearMonth.from(toDateTime())
            return when (depth) {
                MONTH -> ym.plusMonths(1)
                QUARTER -> ym.withMonth(ym.month.firstMonthOfQuarter().value).plusMonths(3)
                YEAR -> ym.plusYears(1).withMonth(1)
            }.startMicros
        }

        private fun IntArray.recencyPartitions(
            recencies: VectorReader,
            depth: RecencyGranularity,
        ): SortedMap<InstantMicros, Selection> {
            val res = sortedMapOf<InstantMicros, IntArrayList>()

            for (idx in this) {
                val recency = recencies.getLong(idx).recencyBucket(depth)
                res.computeIfAbsent(recency) { IntArrayList() }.add(idx)
            }

            return res.mapValuesTo(sortedMapOf()) { it.value.toArray() }
        }

        private fun IntArray.recencyPartitions(
            recencies: VectorReader,
            pageLimit: Int,
        ): SortedMap<InstantMicros, Selection> {
            if (isEmpty()) return sortedMapOf()

            val partCount = ceil(size.toDouble() / pageLimit).toInt()
            val partSize = ceil(size.toDouble() / partCount).toInt()

            val sortedByRecency =
                Arrays.stream(this).boxed()
                    .sorted { l, r -> recencies.getLong(r).compareTo(recencies.getLong(l)) }
                    .mapToInt { it }
                    .toArray()

            // split sortedByRecency into sub-arrays of size partSize
            val res = sortedMapOf<InstantMicros, Selection>()

            for (sortedIdx in sortedByRecency.indices step partSize) {
                val part = sortedByRecency.sliceArray(sortedIdx until (sortedIdx + partSize).coerceAtMost(size))
                val recency = recencies.getLong(part.first())
                res[recency] = part.sortedArray() // preserve the initial sort order
            }

            return res
        }

        @Suppress("unused")
        @JvmOverloads
        @JvmStatic
        fun writeRelation(
            trieWriter: TrieWriter,
            relation: RelationReader,
            recencies: VectorReader,
            pageLimit: Int = 256,
        ): FileSize {
            val trieDataRel = trieWriter.dataRel
            val rowCopier = trieDataRel.rowCopier(relation)
            val iidReader = relation["_iid"]!!

            val startPtr = ArrowBufPointer()
            val endPtr = ArrowBufPointer()

            /**
             * Writes a subtree of the trie for a given selection over the relation.
             *
             * If the trie is empty, it is represented as an empty recency branch.
             * If the trie data fits onto a single page, it is represented as a leaf pointing to one page.
             * Otherwise, the trie is split by recency and IID branch interleaving (at the first 3 levels).
             * There should be no recency nodes pointing to an empty subtrie. An IID branch might have a null child,
             * but not every child can be null.
             */

            fun writeSubtree(depth: Int, sel: Selection): Int {

                fun writeRecencyBranch(parts: SortedMap<InstantMicros, Selection>): Int =
                    trieWriter.writeRecencyBranch(
                        parts.mapValuesTo(sortedMapOf()) { innerSel ->
                            writeSubtree(depth + 1, innerSel.value)
                        }
                    )

                return if (Thread.interrupted()) throw InterruptedException()
                else if (sel.isEmpty()) {
                    writeRecencyBranch(sortedMapOf())
                } else if (sel.size <= pageLimit) {
                    for (idx in sel) rowCopier.copyRow(idx)

                    val pos = trieWriter.writeLeaf()
                    trieDataRel.clear()
                    pos
                } else {
                    // Year, IID[0], Quarter, IID[1], Month, IID[2...]
                    // then, when we've run out of IID, or when the first and last IIDs are the same,
                    // we know that it's all versions of the same IID, so we page by recency
                    when (depth) {
                        0 -> writeRecencyBranch(sel.recencyPartitions(recencies, YEAR))
                        2 -> writeRecencyBranch(sel.recencyPartitions(recencies, QUARTER))
                        4 -> writeRecencyBranch(sel.recencyPartitions(recencies, MONTH))

                        else -> {
                            val iidDepth = when (depth) {
                                1 -> 0; 3 -> 1
                                else -> depth - 3
                            }

                            if (iidDepth >= 64 || iidReader.getPointer(sel.first(), startPtr) == iidReader.getPointer(
                                    sel.last(),
                                    endPtr
                                )
                            ) {
                                writeRecencyBranch(sel.recencyPartitions(recencies, pageLimit))
                            } else {
                                trieWriter.writeIidBranch(
                                    sel.iidPartitions(iidReader, iidDepth)
                                        .map { innerSel ->
                                            if (innerSel == null) -1 else writeSubtree(depth + 1, innerSel)
                                        }
                                        .toIntArray())
                            }
                        }
                    }
                }
            }

            writeSubtree(0, IntArray(relation.rowCount) { idx -> idx })

            return trieWriter.end()
        }

        @JvmStatic
        fun open(
            impl: Impl, log: Log, trieCatalog: TrieCatalog,
            ignoreBlockSignal: Boolean = false, threadLimit: Int = 1
        ) =
            object : Compactor {
                private val scope = CoroutineScope(Dispatchers.Default)

                private val jobsScope =
                    CoroutineScope(
                        Dispatchers.Default.limitedParallelism(threadLimit, "compactor")
                                + SupervisorJob(scope.coroutineContext.job)
                    )

                private val wakeup = Channel<Unit>(1, onBufferOverflow = DROP_OLDEST)
                private val idle = Channel<Unit>()

                @Volatile
                private var availableJobKeys = emptySet<String>()

                private val queuedJobs = mutableSetOf<String>()

                init {
                    scope.launch {
                        val doneCh = Channel<Job>()

                        while (true) {
                            val availableJobs = impl.availableJobs()
                            availableJobKeys = availableJobs.map { it.outputTrieKey }.toSet()

                            if (availableJobs.isEmpty() && queuedJobs.isEmpty()) {
                                LOGGER.trace("sending idle")
                                idle.trySend(Unit)
                            }

                            availableJobs.forEach {
                                if (queuedJobs.add(it.outputTrieKey)) {
                                    jobsScope.launch {
                                        // check it's still required
                                        if (it.outputTrieKey in availableJobKeys) {
                                            LOGGER.debug("executing job: ${it.outputTrieKey}")

                                            val res = runInterruptible { impl.executeJob(it) }

                                            // add the trie to the catalog eagerly so that it's present
                                            // next time we run `availableJobs` (it's idempotent)
                                            trieCatalog.addTrie(res)
                                            log.appendMessage(TriesAdded(listOf(res))).await()

                                            LOGGER.debug("done: ${it.outputTrieKey}")
                                        }

                                        doneCh.send(it)
                                    }
                                }
                            }

                            select {
                                doneCh.onReceive {
                                    queuedJobs.remove(it.outputTrieKey)
                                    LOGGER.debug("Completed job ${it.outputTrieKey}")
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

        @JvmStatic
        val noop = object : Compactor {
            override fun signalBlock() {}
            override fun compactAll(timeout: Duration?) {}
            override fun close() {}
        }
    }
}
