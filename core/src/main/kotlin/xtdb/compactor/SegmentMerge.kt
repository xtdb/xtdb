@file:OptIn(ExperimentalPathApi::class)

package xtdb.compactor

import com.carrotsearch.hppc.ByteArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.RowCopier
import xtdb.bitemporal.PolygonCalculator
import xtdb.trie.*
import xtdb.trie.Trie.dataRelSchema
import xtdb.types.Fields.mergeFields
import xtdb.types.withName
import xtdb.util.closeOnCatch
import xtdb.util.openReadableChannel
import xtdb.util.openWritableChannel
import java.nio.file.Path
import java.util.*
import java.util.function.Predicate
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteRecursively
import kotlin.math.min

class SegmentMerge(private val al: BufferAllocator) {

    companion object {
        private fun logDataRelSchema(dataSchemas: Collection<Schema>) =
            mergeFields(dataSchemas.map { it.findField("op").children.first() })
                .withName("put")
                .let { dataRelSchema(it) }

        private fun ByteArray.toPathPredicate() =
            Predicate<ByteArray> { pagePath ->
                val len = min(size, pagePath.size)
                Arrays.equals(this, 0, len, pagePath, 0, len)
            }
    }

    private class CopierFactory(private val dataRel: Relation) {
        private val iidWtr = dataRel["_iid"]!!
        private val sfWtr = dataRel["_system_from"]!!
        private val vfWtr = dataRel["_valid_from"]!!
        private val vtWtr = dataRel["_valid_to"]!!
        private val opWtr = dataRel["op"]!!

        fun rowCopier(dataReader: RelationReader): RowCopier {
            val iidCopier = dataReader["_iid"]!!.rowCopier(iidWtr)
            val sfCopier = dataReader["_system_from"]!!.rowCopier(sfWtr)
            val vfCopier = dataReader["_valid_from"]!!.rowCopier(vfWtr)
            val vtCopier = dataReader["_valid_to"]!!.rowCopier(vtWtr)
            val opCopier = dataReader["op"]!!.rowCopier(opWtr)

            return RowCopier {
                val pos = iidCopier.copyRow(it)

                sfCopier.copyRow(it)
                vfCopier.copyRow(it)
                vtCopier.copyRow(it)
                opCopier.copyRow(it)
                dataRel.endRow()

                pos
            }
        }
    }

    private class PageMerge(dataRel: Relation, private val pathFilter: ByteArray?) {

        private data class QueueElem(val evPtr: EventRowPointer, val rowCopier: RowCopier)

        private fun <N : HashTrie.Node<N>, L : N> MergePlanNode<N, L>.loadDataPage(): RelationReader? =
            segment.dataRel?.loadPage(node)

        private val copierFactory = CopierFactory(dataRel)

        fun execMergeTask(task: MergePlanTask) {
            val polygonCalculator = PolygonCalculator()
            val isValidPtr = ArrowBufPointer()

            val mpNodes = task.mpNodes
            var path = task.path
            val dataReaders = mpNodes.map { it.loadDataPage() }
            val mergeQueue = PriorityQueue(Comparator.comparing(QueueElem::evPtr, EventRowPointer.comparator()))
            path = if (pathFilter == null || path.size > pathFilter.size) path else pathFilter

            for (dataReader in dataReaders) {
                if (dataReader == null) continue
                val evPtr = EventRowPointer.XtArrow(dataReader, path)
                val rowCopier = copierFactory.rowCopier(dataReader)

                if (evPtr.isValid(isValidPtr, path))
                    mergeQueue.add(QueueElem(evPtr, rowCopier))
            }

            var seenErase = false

            while (true) {
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

    class MergeResult(private val path: Path, val leaves: List<PageTree.Leaf>) : AutoCloseable {
        fun openForRead() = path.openReadableChannel()

        override fun close() = path.deleteRecursively()
    }

    internal fun mergeSegments(segments: List<ISegment<*, *>>, pathFilter: ByteArray?): MergeResult {
        val schema = logDataRelSchema(segments.map { it.dataRel!!.schema })
        val mergePlan = segments.toMergePlan(pathFilter?.toPathPredicate())
        val outPath = createTempFile("merged-segments", ".arrow")

        val leaves = Relation(al, schema).use { dataRel ->
            val pageMerge = PageMerge(dataRel, pathFilter)

            dataRel.startUnload(outPath.openWritableChannel()).use { unloader ->
                var idx = 0

                mergePlan
                    .mapNotNull { task ->
                        if (Thread.interrupted()) throw InterruptedException()

                        pageMerge.execMergeTask(task)

                        if (dataRel.rowCount == 0) return@mapNotNull null

                        unloader.writePage()
                        PageTree.Leaf(idx++, ByteArrayList.from(*task.path), dataRel.rowCount)
                            .also { dataRel.clear() }
                    }
                    .also { unloader.end() }
            }
        }

        return MergeResult(outPath, leaves)
    }

    fun mergeToRelation(segments: List<ISegment<*, *>>, part: ByteArray?) =
        mergeSegments(segments, part).use { res ->
            Relation.loader(al, res.openForRead()).use { inLoader ->
                val schema = inLoader.schema
                Relation(al, schema).closeOnCatch { outRel ->
                    Relation(al, schema).use { inRel ->
                        while (inLoader.loadNextPage(inRel))
                            outRel.append(inRel)
                    }

                    outRel
                }
            }
        }
}
