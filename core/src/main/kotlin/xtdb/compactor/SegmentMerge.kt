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

        private fun <N : HashTrie.Node<N>, L : N> MergePlanNode<N, L>.loadDataPage(): RelationReader? =
            segment.dataRel?.loadPage(node)
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

    private data class QueueElem(val evPtr: EventRowPointer, val rowCopier: RowCopier)

    class MergeResult(private val path: Path, val leaves: List<PageTree.Leaf>) : AutoCloseable {
        fun openForRead() = path.openReadableChannel()

        override fun close() = path.deleteRecursively()
    }

    private inner class OutRel(schema: Schema) : AutoCloseable {
        private val outPath = createTempFile("merged-segments", ".arrow")

        private val outRel = Relation(al, schema)

        private val unloader = runCatching { outRel.startUnload(outPath.openWritableChannel()) }
            .onFailure { outRel.close() }
            .getOrThrow()

        private val copierFactory = CopierFactory(outRel)

        private var pageIdx = 0
        private val leaves = mutableListOf<PageTree.Leaf>()

        fun rowCopier(reader: RelationReader) = copierFactory.rowCopier(reader)

        fun endPage(path: ByteArray) {
            if (outRel.rowCount == 0) return

            unloader.writePage()

            leaves += PageTree.Leaf(pageIdx++, ByteArrayList.from(*path), outRel.rowCount)
            outRel.clear()
        }

        fun end(): MergeResult {
            unloader.end()
            return MergeResult(outPath, leaves)
        }

        override fun close() {
            unloader.close()
            outRel.close()
        }
    }

    internal fun mergeSegments(segments: List<ISegment<*, *>>, pathFilter: ByteArray?): MergeResult {
        val schema = logDataRelSchema(segments.map { it.dataRel!!.schema })

        val isValidPtr = ArrowBufPointer()

        return OutRel(schema).use { outWtr ->
            for (task in segments.toMergePlan(pathFilter?.toPathPredicate())) {
                if (Thread.interrupted()) throw InterruptedException()

                val mpNodes = task.mpNodes
                val path = task.path.let { if (pathFilter == null || it.size > pathFilter.size) it else pathFilter }
                val mergeQueue = PriorityQueue(Comparator.comparing(QueueElem::evPtr, EventRowPointer.comparator()))

                for (dataReader in mpNodes.mapNotNull { it.loadDataPage() }) {
                    val evPtr = EventRowPointer.XtArrow(dataReader, path)
                    val rowCopier = outWtr.rowCopier(dataReader)

                    if (evPtr.isValid(isValidPtr, path))
                        mergeQueue.add(QueueElem(evPtr, rowCopier))
                }

                var seenErase = false

                val polygonCalculator = PolygonCalculator()

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

                outWtr.endPage(task.path)
            }

            outWtr.end()
        }
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
