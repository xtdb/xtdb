package xtdb.compactor

import com.carrotsearch.hppc.ByteArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.bitemporal.PolygonCalculator
import xtdb.compactor.OutWriter.OutWriters
import xtdb.compactor.OutWriter.RecencyRowCopier
import xtdb.compactor.RecencyPartition.*
import xtdb.trie.*
import xtdb.trie.Trie.dataRelSchema
import xtdb.types.Fields.mergeFields
import xtdb.types.withName
import xtdb.util.closeOnCatch
import xtdb.util.openReadableChannel
import java.nio.file.Path
import java.time.LocalDate
import java.util.*
import java.util.function.Predicate
import kotlin.io.path.deleteExisting
import kotlin.math.min
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

private fun logDataRelSchema(dataSchemas: Collection<Schema>) =
    mergeFields(dataSchemas.map { it.findField("op").children.first() })
        .withName("put")
        .let { dataRelSchema(it) }

private fun ByteArray.toPathPredicate() =
    Predicate<ByteArray> { pagePath ->
        val len = min(size, pagePath.size)
        Arrays.equals(this, 0, len, pagePath, 0, len)
    }

private fun <N : HashTrie.Node<N>, L : N> MergePlanNode<N, L>.loadDataPage(): RelationReader? =
    segment.dataRel?.loadPage(node)

internal class SegmentMerge(private val al: BufferAllocator) {

    class Result(internal val path: Path, val recency: LocalDate?, val leaves: List<PageTree.Leaf>) : AutoCloseable {
        fun openForRead() = path.openReadableChannel()

        override fun close() = path.deleteExisting()
    }

    /**
     * Closing these results deletes the temporary files.
     */
    class Results(private val results: List<Result>, private val dir: Path? = null) : Iterable<Result>, AutoCloseable {
        override fun iterator() = results.iterator()

        override fun close() {
            results.forEach { it.close() }
            dir?.deleteExisting()
        }
    }

    // for clojure test
    fun Result.openAllAsRelation() =
        Relation.loader(al, openForRead()).use { inLoader ->
            val schema = inLoader.schema
            Relation(al, schema).closeOnCatch { outRel ->
                Relation(al, schema).use { inRel ->
                    while (inLoader.loadNextPage(inRel))
                        outRel.append(inRel)
                }

                outRel
            }
        }

    private data class QueueElem(val evPtr: EventRowPointer, val rowCopier: RecencyRowCopier)

    private val outWriters = OutWriters(al)

    private fun MergePlanTask.merge(outWriter: OutWriter, pathFilter: ByteArray?) {
        val isValidPtr = ArrowBufPointer()

        val mpNodes = mpNodes
        val path = path.let { if (pathFilter == null || it.size > pathFilter.size) it else pathFilter }
        val mergeQueue = PriorityQueue(Comparator.comparing(QueueElem::evPtr, EventRowPointer.comparator()))

        for (dataReader in mpNodes.mapNotNull { it.loadDataPage() }) {
            val evPtr = EventRowPointer.XtArrow(dataReader, path)
            val rowCopier = outWriter.rowCopier(dataReader)

            if (evPtr.isValid(isValidPtr, path))
                mergeQueue.add(QueueElem(evPtr, rowCopier))
        }

        var seenErase = false

        val polygonCalculator = PolygonCalculator()

        while (true) {
            val elem = mergeQueue.poll() ?: break
            val (evPtr, rowCopier) = elem

            when (val polygon = polygonCalculator.calculate(evPtr)) {
                null -> {
                    if (!seenErase) rowCopier.copyRow(MAX_LONG, evPtr.index)
                    seenErase = true
                }

                else -> rowCopier.copyRow(polygon.recency, evPtr.index)
            }

            evPtr.nextIndex()

            if (evPtr.isValid(isValidPtr, path))
                mergeQueue.add(elem)
        }

        outWriter.endPage(ByteArrayList.from(*this.path))
    }

    sealed interface RecencyPartitioning {
        data object Partition : RecencyPartitioning

        class Preserve(val recency: LocalDate?) : RecencyPartitioning
    }

    @JvmOverloads
    fun mergeSegments(
        segments: List<ISegment<*, *>>,
        pathFilter: ByteArray?,
        recencyPartitioning: RecencyPartitioning,
        recencyPartition: RecencyPartition? = WEEK
    ): Results {
        val schema = logDataRelSchema(segments.map { it.dataRel!!.schema })

        val outWriter = when(recencyPartitioning) {
            RecencyPartitioning.Partition -> outWriters.PartitionedOutWriter(schema, recencyPartition)
            is RecencyPartitioning.Preserve -> outWriters.OutRel(schema, recency = recencyPartitioning.recency)
        }

        return outWriter.use {
            for (task in segments.toMergePlan(pathFilter?.toPathPredicate())) {
                if (Thread.interrupted()) throw InterruptedException()

                task.merge(it, pathFilter)
            }

            it.end()
        }
    }
}
