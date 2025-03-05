package xtdb.compactor

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.time.withTimeout
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.api.log.Log
import xtdb.api.log.Log.Message.TriesAdded
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.RowCopier
import xtdb.arrow.VectorReader
import xtdb.bitemporal.PolygonCalculator
import xtdb.log.proto.AddedTrie
import xtdb.trie.*
import xtdb.trie.HashTrie.Companion.LEVEL_WIDTH
import xtdb.util.logger
import xtdb.util.trace
import java.time.Duration
import java.util.*
import java.util.function.Predicate
import kotlin.math.min
import kotlin.time.Duration.Companion.seconds

private typealias InstantMicros = Long
private typealias Selection = IntArray

private typealias JobKey = Pair<TableName, TrieKey>

interface Compactor : AutoCloseable {

    interface Job {
        val tableName: String
        val outputTrieKey: String
    }

    interface Impl : AutoCloseable {
        fun availableJobs(): Collection<Job>
        fun executeJob(job: Job): List<AddedTrie>
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

        private fun ByteArray.toPathPredicate() =
            Predicate<ByteArray> { pagePath ->
                val len = min(size, pagePath.size)
                Arrays.equals(this, 0, len, pagePath, 0, len)
            }

        @Suppress("UNCHECKED_CAST")
        private fun <L : HashTrie.Node<*>> MergePlanNode<L>.loadDataPage(): RelationReader? =
            segment.dataRel?.loadPage(node as L)

        fun copierFactory(dataWriter: Relation): (RelationReader) -> RowCopier {
            val iidWtr = dataWriter["_iid"]!!
            val sfWtr = dataWriter["_system_from"]!!
            val vfWtr = dataWriter["_valid_from"]!!
            val vtWtr = dataWriter["_valid_to"]!!
            val opWtr = dataWriter["op"]!!

            return { dataReader ->
                val iidCopier = dataReader["_iid"]!!.rowCopier(iidWtr)
                val sfCopier = dataReader["_system_from"]!!.rowCopier(sfWtr)
                val vfCopier = dataReader["_valid_from"]!!.rowCopier(vfWtr)
                val vtCopier = dataReader["_valid_to"]!!.rowCopier(vtWtr)
                val opCopier = dataReader["op"]!!.rowCopier(opWtr)

                RowCopier {
                    val pos = iidCopier.copyRow(it)

                    sfCopier.copyRow(it)
                    vfCopier.copyRow(it)
                    vtCopier.copyRow(it)
                    opCopier.copyRow(it)
                    dataWriter.endRow()

                    pos
                }
            }
        }

        @JvmStatic
        fun mergeSegmentsInto(dataRel: Relation, segments: List<ISegment<*>>, pathFilter: ByteArray?) {
            val copierFactory = copierFactory(dataRel)
            val polygonCalculator = PolygonCalculator()
            val isValidPtr = ArrowBufPointer()

            data class QueueElem(val evPtr: EventRowPointer, val rowCopier: RowCopier)

            for (task in toMergePlan(segments, pathFilter?.toPathPredicate())) {
                if (Thread.interrupted()) throw InterruptedException()

                val mpNodes = task.mpNodes
                var path = task.path
                val dataReaders = mpNodes.map { it.loadDataPage() }
                val mergeQueue = PriorityQueue(Comparator.comparing(QueueElem::evPtr, EventRowPointer.comparator()))
                path = if (pathFilter == null || path.size > pathFilter.size) path else pathFilter

                for (dataReader in dataReaders) {
                    if (dataReader == null) continue
                    val evPtr = EventRowPointer.XtArrow(dataReader, path)
                    val rowCopier = copierFactory(dataReader)
                    if (evPtr.isValid(isValidPtr, path)) {
                        mergeQueue.add(QueueElem(evPtr, rowCopier))
                    }
                }

                var seenErase = false
                while(true) {
                    val elem = mergeQueue.poll() ?: break
                    val (evPtr, rowCopier) = elem
                    if (polygonCalculator.calculate(evPtr) != null) {
                        rowCopier.copyRow(evPtr.index)
                    } else {
                        if (!seenErase) rowCopier.copyRow(evPtr.index)
                        seenErase = true
                    }

                    evPtr.nextIndex()
                    if (evPtr.isValid(isValidPtr, path)) {
                        mergeQueue.add(elem)
                    }
                }
            }
        }

        @Suppress("unused")
        @JvmOverloads
        @JvmStatic
        fun writeRelation(
            trieWriter: TrieWriter,
            relation: RelationReader,
            pageLimit: Int = 256,
        ): FileSize {
            val trieDataRel = trieWriter.dataRel
            val rowCopier = trieDataRel.rowCopier(relation)
            val iidReader = relation["_iid"]!!

            val startPtr = ArrowBufPointer()
            val endPtr = ArrowBufPointer()

            fun Selection.soloIid(): Boolean =
                iidReader.getPointer(first(), startPtr) == iidReader.getPointer(last(), endPtr)

            fun writeSubtree(depth: Int, sel: Selection): Int {

                return when {
                    Thread.interrupted() -> throw InterruptedException()

                    sel.isEmpty() -> trieWriter.writeNull()

                    sel.size <= pageLimit || depth >= 64 || sel.soloIid() -> {
                        for (idx in sel) rowCopier.copyRow(idx)

                        val pos = trieWriter.writeLeaf()
                        trieDataRel.clear()
                        pos
                    }

                    else ->
                        trieWriter.writeIidBranch(
                            sel.iidPartitions(iidReader, depth)
                                .map { if (it != null) writeSubtree(depth + 1, it) else -1 }
                                .toIntArray())
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
                private var availableJobs = emptyMap<JobKey, Job>()

                private val queuedJobs = mutableSetOf<JobKey>()

                init {
                    scope.launch {
                        val doneCh = Channel<JobKey>()

                        while (true) {
                            availableJobs = impl.availableJobs().associateBy { JobKey(it.tableName, it.outputTrieKey) }

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
                                            val res = runInterruptible { impl.executeJob(job) }

                                            // add the trie to the catalog eagerly so that it's present
                                            // next time we run `availableJobs` (it's idempotent)
                                            trieCatalog.addTries(res)
                                            log.appendMessage(TriesAdded(res)).await()
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
                    impl.close()
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
