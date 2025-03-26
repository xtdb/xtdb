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
import kotlin.Long.Companion.MAX_VALUE
import kotlin.Long.Companion.MIN_VALUE
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

internal class SegmentMerge(private val al: BufferAllocator) : AutoCloseable {

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

    private fun resolveSameSystemTimeEvents(dataReader: RelationReader, path: ByteArray) : RelationReader {
        val isValidPtr = ArrowBufPointer()
        val startIidPtr = ArrowBufPointer()
        val curIidPtr = ArrowBufPointer()
        var curSystemFrom : Long

        val relWriter = Relation(al, dataReader.schema)
        val iidVec = dataReader["_iid"].rowCopier(relWriter["_iid"])
        val sysFromVec= dataReader["_system_from"].rowCopier(relWriter["_system_from"])
        val validFromVec = relWriter["_valid_from"]
        val validToVec = relWriter["_valid_to"]
        val opCopier = dataReader["op"].rowCopier(relWriter["op"])

        val evPtr = EventRowPointer(dataReader, path)
        val polygonCalculator = PolygonCalculator()

        var seenErase = false

        while (evPtr.isValid(isValidPtr, path)) {
            evPtr.getIidPointer(startIidPtr)
            curSystemFrom = evPtr.systemFrom

            while(evPtr.isValid(isValidPtr, path) && evPtr.systemFrom == curSystemFrom && startIidPtr == evPtr.getIidPointer(curIidPtr)) {

                when (val polygon = polygonCalculator.calculate(evPtr)) {
                    // here we are only taking care of an erase that happens within the same transaction
                    null -> {
                        if (!seenErase) {
                            iidVec.copyRow(evPtr.index)
                            sysFromVec.copyRow(evPtr.index)
                            validFromVec.writeLong(MIN_VALUE)
                            validToVec.writeLong(MAX_VALUE)
                            opCopier.copyRow(evPtr.index)
                            relWriter.endRow()
                        }
                        seenErase = true
                    }
                    else -> {
                        repeat(polygon.validTimeRangeCount) { i ->
                            if (polygon.getSystemTo(i) > curSystemFrom) {
                                iidVec.copyRow(evPtr.index)
                                sysFromVec.copyRow(evPtr.index)
                                validFromVec.writeLong(polygon.getValidFrom(i))
                                validToVec.writeLong(polygon.getValidTo(i))
                                opCopier.copyRow(evPtr.index)
                                relWriter.endRow()
                            }
                        }
                    }
                }
                evPtr.nextIndex()
            }
            polygonCalculator.reset()
            seenErase = false
        }
        return relWriter
    }

    private fun MergePlanTask.merge(outWriter: OutWriter, pathFilter: ByteArray?, bitemporalResolution: Boolean = false) {
        val isValidPtr = ArrowBufPointer()

        val mpNodes = mpNodes
        val path = path.let { if (pathFilter == null || it.size > pathFilter.size) it else pathFilter }
        val mergeQueue = PriorityQueue(Comparator.comparing(QueueElem::evPtr, EventRowPointer.comparator()))

        val dataReaders = mutableListOf<RelationReader>()

        try {
            for (dataReader in mpNodes.mapNotNull { it.loadDataPage() }) {
                val dataReader = dataReader.run {
                    if (bitemporalResolution) resolveSameSystemTimeEvents(this, path).also { dataReaders.add(it) }
                    else this
                }
                val evPtr = EventRowPointer(dataReader , path)
                val rowCopier = outWriter.rowCopier(dataReader)

                if (evPtr.isValid(isValidPtr, path))
                    mergeQueue.add(QueueElem(evPtr, rowCopier))
            }

            var seenErase = false
            val iidPtr = ArrowBufPointer()

            val polygonCalculator = PolygonCalculator()

            while (true) {
                val elem = mergeQueue.poll() ?: break
                val (evPtr, rowCopier) = elem

                if (polygonCalculator.currentIidPtr != evPtr.getIidPointer(iidPtr)) seenErase = false

                when (val polygon = polygonCalculator.calculate(evPtr)) {
                    null -> {
                        if (!seenErase) rowCopier.copyRow(MAX_LONG, evPtr.index)
                        seenErase = true
                    }

                    else ->  rowCopier.copyRow(polygon.recency, evPtr.index)
                }

                evPtr.nextIndex()

                if (evPtr.isValid(isValidPtr, path))
                    mergeQueue.add(elem)
            }

            if (bitemporalResolution) dataReaders.forEach { it.close() }

            outWriter.endPage(ByteArrayList.from(*this.path))

        } catch (e: Throwable) {
            if (bitemporalResolution) dataReaders.forEach { it.close() }
            throw e
        }
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

        val (outWriter, bitemporalResolution)= when(recencyPartitioning) {
            RecencyPartitioning.Partition -> outWriters.PartitionedOutWriter(schema, recencyPartition) to true
            is RecencyPartitioning.Preserve -> outWriters.OutRel(schema, recency = recencyPartitioning.recency) to false
        }

        return outWriter.use {
            for (task in segments.toMergePlan(pathFilter?.toPathPredicate())) {
                if (Thread.interrupted()) throw InterruptedException()

                task.merge(it, pathFilter, bitemporalResolution)
            }

            it.end()
        }
    }

    override fun close() = outWriters.close()
}
